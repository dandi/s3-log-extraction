"""
Per-IP behavioral profiler: separate authentic use from bots by *irregularity*.

The organizing principle (see analysis notes): authentic human use is irregular in
timing and idiosyncratic in which assets it chooses; a bot is systematic — regular
timing and/or a uniform, exhaustive, or fixed asset selection. Breadth, size, and depth
do NOT separate them (a metadata scraper or checksum scanner touches *every* asset), so
this profiler measures the two axes that do:

  * timing irregularity — coefficient of variation of the gaps between an IP's view
    sessions, and the fraction of those gaps clustered at a dominant period. Low CV or a
    strong dominant period = metronomic = bot.
  * archive coverage — the fraction of ALL assets an IP touched. A human touches a tiny
    fraction; a systematic scanner (metadata scrub, checksum sweep, mirror) touches an
    implausibly large one. This is the "enumeration" signal.

Read shape (mean bytes per session) is carried as a descriptor — a checksum scanner reads
whole files, a metadata scraper reads headers — but it is not a decision axis on its own.

The per-asset session-count entropy (``selection_entropy``) is ALSO computed and kept in the
table, but the first real run showed it is ~1.0 for nearly every high-activity IP — because
one-session-per-asset is the norm, so touching many assets once each looks "uniform" whether
it is a scanner or a researcher with broad interests. It measures revisit-evenness, not the
randomness of which assets are chosen, so it is a descriptor only, NOT a decision axis.

This is a CHARACTERIZATION tool, not a shipped classifier: it emits a per-IP feature table
and a summary so the archetypes (pollers, scrapers, checksum scanners, real users) can be
seen as clusters before any exclusion rule is written.

Sessions, not requests
----------------------
Events are *view sessions* (the shipped ``number_of_views`` unit: a maximal run of
streaming requests from one IP to one asset with no gap > 8 h), so the profile matches the
metric we would clean, and single-touch assets are kept (a scraper's one-request-per-asset
sweeps are exactly what a request-count floor would hide).

Two-tier cost control
----------------------
Cheap features (counts, coverage, selection entropy, read-shape, diurnal spread) are
computed for EVERY IP. The timing features (gap-CV, dominant period) need an IP's sorted
session times and are reported for IPs with ``>= --min-sessions`` sessions (default 20);
below that an IP is too low-volume to be a meaningful bot or to move the view count. The
floor is a compute bound, never the bot decision — a high-throughput real user is profiled
and kept on its irregularity, not dropped for its count.

Usage
-----
    python profile_ip_behavior.py --cache-dir /path/to/cache [--no-encryption] \\
        [--testing-asset-file analysis/testing_blobs.txt] [--cache-parquet] \\
        [--min-sessions 20] [--out ip_behavior.png]

The resolver (labels) needs the GeoLite2 database + network for service ranges, as
``update summaries`` does. IPs are stored only as a salted keyed hash.
"""

import argparse
import collections
import fnmatch
import hashlib
import math
import os
import pathlib
import sys

import numpy as np
import pandas as pd
import tqdm

_SERVICES = ("GH-actions", "GitHub", "AWS", "GCP", "VPN")


def _load_library():
    """Import the production timestamp format, session timeout, resolver, and IP reader."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
    from s3_log_extraction.ip_utils import IpRegionResolver
    from s3_log_extraction.ip_utils._ip_utils import _read_ips_from_file
    from s3_log_extraction.summarize.globals import SESSION_TIMEOUT_IN_SECONDS, TIMESTAMP_FORMAT

    return IpRegionResolver, _read_ips_from_file, SESSION_TIMEOUT_IN_SECONDS, TIMESTAMP_FORMAT


def _ip_hash_key() -> bytes:
    salt = (
        os.environ.get("S3_LOG_EXTRACTION_SALT") or os.environ.get("S3_LOG_EXTRACTION_PASSWORD") or "s3_log_extraction"
    )
    return salt.encode("utf-8")[:64]


def _service_of(label: str) -> str:
    if not label:
        return "geographic"
    head = label.split("/", 1)[0]
    return head if head in _SERVICES else "geographic"


def _load_globs(path: pathlib.Path | None) -> list[str]:
    if path is None:
        return []
    return [
        stripped
        for line in path.read_text().splitlines()
        if (stripped := line.strip()) and not stripped.startswith("#")
    ]


def _session_starts(epochs: list[int], session_timeout_in_seconds: int) -> list[int]:
    """Session-start epochs for one (IP, asset): the first request, then each request more
    than ``session_timeout_in_seconds`` after the previous one (the shipped view definition)."""
    if not epochs:
        return []
    ordered = sorted(epochs)
    starts = [ordered[0]]
    starts.extend(
        current for previous, current in zip(ordered, ordered[1:]) if current - previous > session_timeout_in_seconds
    )
    return starts


def _selection_entropy(per_asset_session_counts: list[int]) -> float:
    """
    Normalized Shannon entropy (0..1) of how an IP's sessions spread across its assets.

    1.0 = perfectly uniform across the assets it touches (the same treatment applied to each
    — a systematic sweep / scraper); low = concentrated on a few (a human returning to files
    of interest, or a single-asset poller). Undefined for a single asset, reported as 0.0
    (maximally concentrated).
    """
    total = sum(per_asset_session_counts)
    k = len(per_asset_session_counts)
    if k <= 1 or total <= 0:
        return 0.0
    probabilities = [count / total for count in per_asset_session_counts if count > 0]
    entropy = -sum(p * math.log(p) for p in probabilities)
    return entropy / math.log(k)


def _timing_features(session_epochs: list[int], dominant_tol: float = 0.1) -> tuple[float, float, float]:
    """(gap CV, dominant-period fraction, median gap in hours) over an IP's sorted session starts."""
    if len(session_epochs) < 3:
        return float("nan"), float("nan"), float("nan")
    gaps = np.diff(np.array(sorted(session_epochs), dtype=np.float64))
    gaps = gaps[gaps > 0]
    if gaps.size < 2:
        return float("nan"), float("nan"), float("nan")
    mean_gap = float(gaps.mean())
    cv = float(gaps.std() / mean_gap) if mean_gap > 0 else float("nan")
    median_gap = float(np.median(gaps))
    dominant = (
        float(np.mean(np.abs(gaps - median_gap) <= dominant_tol * median_gap)) if median_gap > 0 else float("nan")
    )
    return cv, dominant, median_gap / 3600.0


def ip_features(record: dict, total_assets: int, min_sessions: int) -> dict:
    """
    Turn one IP's accumulated activity into the feature row. ``record`` carries
    ``session_epochs`` (list), ``asset_sessions`` (asset -> session count),
    ``total_bytes``, ``n_streaming_requests``, ``testing_sessions``.
    """
    per_asset = list(record["asset_sessions"].values())
    n_sessions = sum(per_asset)
    n_assets = len(per_asset)
    cv, dominant, median_gap_h = (
        _timing_features(record["session_epochs"]) if n_sessions >= min_sessions else (float("nan"),) * 3
    )
    return {
        "n_sessions": n_sessions,
        "n_distinct_assets": n_assets,
        "coverage_fraction": n_assets / total_assets if total_assets else float("nan"),
        "selection_entropy": _selection_entropy(per_asset),
        "session_gap_cv": cv,
        "dominant_period_fraction": dominant,
        "median_session_gap_hours": median_gap_h,
        "mean_session_bytes": record["total_bytes"] / n_sessions if n_sessions else 0.0,
        "streaming_requests": record["n_streaming_requests"],
        "testing_fraction": record["testing_sessions"] / n_sessions if n_sessions else 0.0,
    }


def build_ip_profiles(
    cache_dir: pathlib.Path,
    use_encryption: bool,
    resolver,
    read_ips,
    session_timeout_in_seconds: int,
    timestamp_format: str,
    testing_globs: list[str],
    min_sessions: int,
    max_assets: int | None = None,
) -> pd.DataFrame:
    """Walk the cache once, accumulate per-IP session activity, and return the per-IP feature table."""
    import datetime

    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {cache_dir}")

    asset_dirs = [
        asset_dir
        for dataset_dir in sorted(extraction_root.iterdir())
        if dataset_dir.is_dir()
        for asset_dir in dataset_dir.rglob("*")
        if (asset_dir / "timestamps.txt").exists()
    ]
    if max_assets is not None:
        asset_dirs = asset_dirs[:max_assets]
    total_assets = len(asset_dirs)
    print(f"Found {total_assets} asset directories")

    def _new_record() -> dict:
        return {
            "session_epochs": [],
            "asset_sessions": collections.defaultdict(int),
            "total_bytes": 0,
            "n_streaming_requests": 0,
            "testing_sessions": 0,
        }

    records: dict[str, dict] = collections.defaultdict(_new_record)
    skipped = 0
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Profiling assets"):
        relative_path = asset_dir.relative_to(extraction_root).as_posix()
        is_testing = any(fnmatch.fnmatch(relative_path, glob) for glob in testing_globs)
        try:
            timestamps = [s for line in (asset_dir / "timestamps.txt").read_text().splitlines() if (s := line.strip())]
            downloads = [s for line in (asset_dir / "download.txt").read_text().splitlines() if (s := line.strip())]
            ips = read_ips(file_path=asset_dir / "ips.txt", use_encryption=use_encryption)
            bytes_sent = [s for line in (asset_dir / "bytes_sent.txt").read_text().splitlines() if (s := line.strip())]
        except FileNotFoundError:
            skipped += 1
            continue
        if not (len(timestamps) == len(downloads) == len(ips) == len(bytes_sent)):
            skipped += 1
            continue

        # Collect this asset's streaming (206) requests per IP.
        per_ip_epochs: dict[str, list[int]] = collections.defaultdict(list)
        per_ip_bytes: dict[str, int] = collections.defaultdict(int)
        per_ip_requests: dict[str, int] = collections.defaultdict(int)
        for timestamp, download, ip, n_bytes in zip(timestamps, downloads, ips, bytes_sent):
            if download != "0":  # streaming only
                continue
            epoch = int(
                datetime.datetime.strptime(timestamp, timestamp_format)
                .replace(tzinfo=datetime.timezone.utc)
                .timestamp()
            )
            per_ip_epochs[ip].append(epoch)
            per_ip_bytes[ip] += int(n_bytes)
            per_ip_requests[ip] += 1

        for ip, epochs in per_ip_epochs.items():
            starts = _session_starts(epochs, session_timeout_in_seconds)
            if not starts:
                continue
            record = records[ip]
            record["session_epochs"].extend(starts)
            record["asset_sessions"][relative_path] += len(starts)
            record["total_bytes"] += per_ip_bytes[ip]
            record["n_streaming_requests"] += per_ip_requests[ip]
            if is_testing:
                record["testing_sessions"] += len(starts)
    if skipped:
        print(f"  Skipped {skipped} asset(s) with missing or misaligned files")

    print(f"Resolving {len(records):,} distinct IPs...")
    key = _ip_hash_key()
    rows = []
    for ip, record in tqdm.tqdm(records.items(), desc="Resolving + featurizing"):
        features = ip_features(record, total_assets=total_assets, min_sessions=min_sessions)
        label = resolver.resolve(ip) or ""
        rows.append(
            {
                "ip_hash": hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest(),
                "region_label": label,
                "service": _service_of(label),
                **features,
            }
        )
    return pd.DataFrame(rows)


def report(profiles: pd.DataFrame, min_sessions: int, top_n: int = 25) -> None:
    total_sessions = int(profiles["n_sessions"].sum())
    print(f"\n=== Per-IP behavioral profiles ({len(profiles):,} IPs, {total_sessions:,} sessions) ===")

    # --- Concentration (threshold-free, robust): a few IPs may dominate the whole view count. ---
    ordered = profiles["n_sessions"].sort_values(ascending=False).to_numpy()
    cumulative = ordered.cumsum()
    print("\n  session concentration (share of ALL sessions held by the top IPs):")
    for k in [1, 5, 10, 25, 100]:
        if k <= len(ordered):
            print(f"    top {k:>4}: {100 * cumulative[k - 1] / max(total_sessions, 1):5.1f}%")

    active = profiles[profiles["n_sessions"] >= min_sessions].copy()
    print(f"\n  IPs with >= {min_sessions} sessions (timing-scored): {len(active):,}")
    if active.empty:
        print("  (none active enough to score timing)")
        return

    # --- Two SOUND axes (selection_entropy is a descriptor only — see the note below). ---
    # 1) Metronomic timing: a fixed cadence between sessions.
    metronomic = (active["session_gap_cv"] <= 0.1) | (active["dominant_period_fraction"] >= 0.6)
    m_sessions = int(active.loc[metronomic, "n_sessions"].sum())
    print(
        f"\n  metronomic timing (CV<=0.1 or dominant>=0.6): {int(metronomic.sum()):,} IPs, "
        f"{m_sessions:,} sessions ({100 * m_sessions / max(total_sessions, 1):.2f}%)"
    )

    # 2) Archive coverage: the fraction of ALL assets an IP touched. A human touches a tiny
    #    fraction; a systematic scanner touches an implausibly large one. This is the real
    #    "enumeration" signal (selection_entropy is ~1 for almost every high-activity IP, since
    #    one-session-per-asset is the norm, so it does NOT separate scanners from broad humans).
    cov = active["coverage_fraction"]
    print("\n  archive-coverage distribution among active IPs (fraction of all assets touched):")
    for q in [0.5, 0.9, 0.99, 0.999, 1.0]:
        print(f"    {q * 100:>5g}th pct: {cov.quantile(q):.5f}")
    print("\n  IPs above coverage thresholds (systematic-enumeration candidates):")
    for threshold in [0.005, 0.01, 0.02, 0.05, 0.10]:
        mask = cov >= threshold
        s = int(active.loc[mask, "n_sessions"].sum())
        print(
            f"    coverage >= {threshold:>5.1%}: {int(mask.sum()):>5,} IPs, "
            f"{s:>12,} sessions ({100 * s / max(total_sessions, 1):5.1f}% of all)"
        )

    # Combined systematic candidate = metronomic OR broadly-covering, at an illustrative 2% coverage.
    systematic = metronomic | (cov >= 0.02)
    sys_sessions = int(active.loc[systematic, "n_sessions"].sum())
    print(
        f"\n  systematic (metronomic OR coverage>=2%): {int(systematic.sum()):,} IPs, "
        f"{sys_sessions:,} sessions ({100 * sys_sessions / max(total_sessions, 1):.2f}% of all)"
    )

    # --- Label overlay: which SERVICE the systematic, high-volume actors resolve to. The point is
    #     that the biggest scanners are geographic, invisible to the GH-actions and cloud/VPN filters. ---
    print("\n  systematic candidates by service label (sessions):")
    sys_rows = active[systematic]
    by_service = sys_rows.groupby("service")["n_sessions"].sum().sort_values(ascending=False)
    for service, s in by_service.items():
        print(f"    {service:>10}: {int(s):>12,} ({100 * s / max(sys_sessions, 1):5.1f}% of systematic)")

    # --- Do the systematic actors also touch the reserved testing assets? A broad scanner sweeps
    #     the whole archive, so it should hit the testing blobs too (at a tiny testing FRACTION,
    #     since those are a handful of assets among thousands). "Touches" = any testing session. ---
    if "testing_fraction" in active.columns and (active["testing_fraction"] > 0).any():
        broad = active[cov >= 0.02]
        touch = broad[broad["testing_fraction"] > 0]
        print(f"\n  of the {len(broad):,} coverage>=2% IPs, {len(touch):,} also touch the testing assets:")
        for row in broad.sort_values("coverage_fraction", ascending=False).itertuples(index=False):
            testing_sessions = int(round(row.testing_fraction * row.n_sessions))
            flag = f"{testing_sessions:,} testing sessions" if testing_sessions else "none"
            print(f"    {row.region_label or '(unresolved)':<16} cov={row.coverage_fraction:>6.3f}  {flag}")

    print(f"\n  top {top_n} active IPs by sessions (label | sessions | assets | cov | test% | CV | domP | MB/sess):")
    cols = [
        "region_label",
        "n_sessions",
        "n_distinct_assets",
        "coverage_fraction",
        "testing_fraction",
        "session_gap_cv",
        "dominant_period_fraction",
        "mean_session_bytes",
    ]
    for row in active.nlargest(top_n, "n_sessions")[cols].itertuples(index=False):
        label, n, assets, cov_frac, test_frac, cv, dom, mb = row
        print(
            f"    {label or '(unresolved)':<16} {int(n):>7,} {int(assets):>6} {cov_frac:>7.4f} "
            f"{100 * test_frac:>6.3f} {cv:>7.3f} {dom:>6.2f} {mb / 1e6:>8.2f}"
        )


def plot(profiles: pd.DataFrame, min_sessions: int, out_path: pathlib.Path) -> None:
    import matplotlib.pyplot as plt

    active = profiles[(profiles["n_sessions"] >= min_sessions) & profiles["session_gap_cv"].notna()]
    if active.empty:
        print("  (nothing to plot)")
        return
    fig, ax = plt.subplots(figsize=(8.5, 6.5))
    sizes = 6 + 30 * np.log10(active["n_sessions"].clip(lower=1))
    scatter = ax.scatter(
        active["coverage_fraction"].clip(lower=1e-6),
        active["session_gap_cv"].clip(lower=1e-3),
        c=np.log10(active["n_sessions"].clip(lower=1)),
        s=sizes,
        cmap="viridis",
        alpha=0.6,
        linewidths=0,
    )
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("archive coverage fraction  (right = systematic enumeration / scanner)")
    ax.set_ylabel("session gap CV  (low = metronomic / bot, high = irregular / human)")
    ax.set_title(
        f"Per-IP behavior ({len(active):,} IPs ≥ {min_sessions} sessions)\ncolor = log₁₀ sessions", fontsize=10
    )
    ax.axhline(0.1, color="crimson", ls="--", lw=0.8)
    ax.axvline(0.02, color="crimson", ls="--", lw=0.8)
    fig.colorbar(scatter, ax=ax, label="log₁₀ sessions")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def plot_distributions(profiles: pd.DataFrame, min_sessions: int, out_path: pathlib.Path) -> None:
    """
    The distributions that would justify (or refute) a threshold on each axis.

    A defensible cut needs a visible *gap or valley* separating a human bulk from a bot tail — the
    same standard the 8-hour session boundary met. If an axis is a smooth heavy tail with no valley,
    any fixed cut on it is arbitrary, and that is itself the finding.
    """
    import matplotlib.pyplot as plt

    active = profiles[profiles["n_sessions"] >= min_sessions].copy()
    if active.empty:
        print("  (nothing to plot)")
        return
    active["revisit"] = active["n_sessions"] / active["n_distinct_assets"].clip(lower=1)

    fig, axes = plt.subplots(1, 3, figsize=(16.5, 5.0))
    fig.suptitle(
        f"Threshold justification: per-IP distributions ({len(active):,} IPs ≥ {min_sessions} sessions)",
        fontsize=12,
        fontweight="bold",
    )

    # (b) coverage — CCDF on log-log; a scanner tail should break away from the human bulk.
    ax = axes[0]
    cov = np.sort(active["coverage_fraction"].clip(lower=1e-6).to_numpy())[::-1]
    ccdf = np.arange(1, cov.size + 1) / cov.size
    ax.plot(cov, ccdf, lw=1.2, color="steelblue")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.axvline(0.01, color="gray", ls=":", lw=0.8, label="1% (99th pct)")
    ax.axvline(0.02, color="crimson", ls="--", lw=0.9, label="2% (candidate)")
    ax.set_xlabel("archive coverage fraction")
    ax.set_ylabel("fraction of IPs with coverage ≥ x (CCDF)")
    ax.set_title("(b) coverage — is there a gap to cut at?", fontsize=9)
    ax.legend(fontsize=8)

    # (a) timing — CV histogram in log10; a valley near the cut would justify it.
    ax = axes[1]
    cv = active["session_gap_cv"].to_numpy(float)
    cv = cv[np.isfinite(cv) & (cv > 0)]
    if cv.size:
        ax.hist(np.log10(cv), bins=60, color="darkorange", alpha=0.85, edgecolor="none")
    ax.axvline(np.log10(0.1), color="crimson", ls="--", lw=0.9, label="CV = 0.1 (candidate)")
    ax.set_xlabel("session-gap CV (log₁₀)")
    ax.set_ylabel("IPs")
    ax.set_title("(a) timing — is there a valley at the cut?", fontsize=9)
    ax.legend(fontsize=8)

    # revisit rate — the re-indexer tail (sessions per distinct asset).
    ax = axes[2]
    rev = active["revisit"].to_numpy(float)
    rev = rev[np.isfinite(rev) & (rev > 0)]
    if rev.size:
        ax.hist(np.log10(rev), bins=60, color="seagreen", alpha=0.85, edgecolor="none")
    ax.axvline(np.log10(1.0), color="gray", ls=":", lw=0.8, label="1 (touch once)")
    ax.set_xlabel("revisit rate = sessions ÷ distinct assets (log₁₀)")
    ax.set_ylabel("IPs")
    ax.set_title("revisit — re-indexer tail", fontsize=9)
    ax.legend(fontsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def _cache_paths(cache_dir: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    base = cache_dir / "analysis_cache"
    return base / "ip_behavior_profiles.parquet", base / "ip_behavior_profiles.csv.gz"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--testing-asset-file", type=pathlib.Path, default=None)
    parser.add_argument("--min-sessions", type=int, default=20, help="Timing-score IPs with >= this many sessions")
    parser.add_argument("--cache-parquet", action="store_true", help="Cache the per-IP table (salted IP hash) in cache")
    parser.add_argument("--rebuild-cache", action="store_true")
    parser.add_argument("--max-assets", type=int, default=None, help="Layout-independent smoke test over the first N")
    parser.add_argument("--out", type=pathlib.Path, default=pathlib.Path("ip_behavior.png"))
    args = parser.parse_args()

    resolver_cls, read_ips, session_timeout, timestamp_format = _load_library()
    parquet_path, csv_path = _cache_paths(args.cache_dir)

    profiles = None
    if args.cache_parquet and not args.rebuild_cache:
        if parquet_path.exists():
            try:
                profiles = pd.read_parquet(parquet_path)
            except ImportError:
                profiles = None
        if profiles is None and csv_path.exists():
            profiles = pd.read_csv(csv_path, keep_default_na=False)
        if profiles is not None:
            print(f"Loaded {len(profiles):,} per-IP profiles from analysis_cache (skipped the walk)")

    if profiles is None:
        print("Building the production region resolver (GeoLite2 + service ranges)...")
        with resolver_cls(cache_directory=args.cache_dir) as resolver:
            profiles = build_ip_profiles(
                cache_dir=args.cache_dir,
                use_encryption=not args.no_encryption,
                resolver=resolver,
                read_ips=read_ips,
                session_timeout_in_seconds=session_timeout,
                timestamp_format=timestamp_format,
                testing_globs=_load_globs(args.testing_asset_file),
                min_sessions=args.min_sessions,
                max_assets=args.max_assets,
            )
        if args.cache_parquet and not profiles.empty:
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                profiles.to_parquet(parquet_path, index=False)
                written = parquet_path
            except ImportError:
                profiles.to_csv(csv_path, index=False, compression="gzip")
                written = csv_path
            print(f"Cached {len(profiles):,} per-IP profiles to {written} (IPs stored as a salted hash)")

    if profiles.empty:
        print("No IPs found.")
        return

    report(profiles, min_sessions=args.min_sessions)
    try:
        plot(profiles, min_sessions=args.min_sessions, out_path=args.out)
        distributions_path = args.out.with_name(f"{args.out.stem}_distributions{args.out.suffix}")
        plot_distributions(profiles, min_sessions=args.min_sessions, out_path=distributions_path)
    except ImportError:
        print("  (matplotlib not installed — skipped the plots)")


if __name__ == "__main__":
    main()

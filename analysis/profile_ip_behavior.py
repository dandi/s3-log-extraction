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


def _sessions(epochs: list[int], session_timeout_in_seconds: int) -> list[tuple[int, int]]:
    """
    Sessions for one (IP, asset) as ``(start, end)`` epoch pairs — the shipped view definition:
    a maximal run of requests with no consecutive gap over ``session_timeout_in_seconds``.
    ``start`` is the first request of the run, ``end`` the last (equal for a single-request session).
    """
    if not epochs:
        return []
    ordered = sorted(epochs)
    sessions = []
    start = prev = ordered[0]
    for current in ordered[1:]:
        if current - prev > session_timeout_in_seconds:
            sessions.append((start, prev))
            start = current
        prev = current
    sessions.append((start, prev))
    return sessions


def _visits(events: list[tuple[int, int, int]], session_timeout_in_seconds: int) -> list[dict]:
    """
    IP-level visits from an IP's per-(asset) sessions, each a dict
    ``{"start", "duration", "n_files", "n_new"}``.

    ``events`` are ``(start, end, asset_index)`` triples across all of the IP's assets. Sorted by
    start, a new visit begins whenever a session starts more than ``session_timeout_in_seconds`` after
    the running end of the current visit; a visit's files are the distinct assets it spans, and
    ``n_new`` is how many of those assets the IP had never touched in any earlier visit (a metadata
    sweep marches through fresh assets, ``n_new`` ~= ``n_files``; a returning analyst re-touches known
    files, ``n_new`` << ``n_files``).
    """
    if not events:
        return []
    ordered = sorted(events)
    visits = []
    seen: set[int] = set()

    def _close(v_start: int, v_end: int, v_assets: set[int]) -> dict:
        n_new = len(v_assets - seen)
        seen.update(v_assets)
        return {"start": v_start, "duration": float(v_end - v_start), "n_files": len(v_assets), "n_new": n_new}

    v_start, v_end = ordered[0][0], ordered[0][1]
    v_assets = {ordered[0][2]}
    for start, end, asset_index in ordered[1:]:
        if start - v_end > session_timeout_in_seconds:
            visits.append(_close(v_start, v_end, v_assets))
            v_start, v_end, v_assets = start, end, {asset_index}
        else:
            v_end = max(v_end, end)
            v_assets.add(asset_index)
    visits.append(_close(v_start, v_end, v_assets))
    return visits


_ALIAS_ADJECTIVES = (
    "Swift",
    "Silent",
    "Brave",
    "Clever",
    "Gentle",
    "Bold",
    "Calm",
    "Eager",
    "Fierce",
    "Jolly",
    "Keen",
    "Lucid",
    "Merry",
    "Noble",
    "Proud",
    "Quick",
    "Rapid",
    "Sage",
    "Tidy",
    "Vivid",
    "Witty",
    "Zesty",
    "Amber",
    "Cobalt",
    "Crimson",
    "Golden",
    "Ivory",
    "Jade",
    "Scarlet",
    "Teal",
)
_ALIAS_NOUNS = (
    "Otter",
    "Falcon",
    "Maple",
    "Heron",
    "Lynx",
    "Badger",
    "Cedar",
    "Finch",
    "Willow",
    "Marten",
    "Osprey",
    "Bison",
    "Comet",
    "Harbor",
    "Juniper",
    "Kestrel",
    "Lark",
    "Meadow",
    "Nimbus",
    "Opal",
    "Pine",
    "Quartz",
    "Raven",
    "Sable",
    "Thistle",
    "Umber",
    "Vireo",
    "Walrus",
    "Yarrow",
    "Zephyr",
)


def _alias(ip_hash: str) -> str:
    """A deterministic CamelCase pseudonym for a hashed IP (stable across runs, human-readable)."""
    value = int(ip_hash[:12], 16)
    adjective = _ALIAS_ADJECTIVES[value % len(_ALIAS_ADJECTIVES)]
    noun = _ALIAS_NOUNS[(value // len(_ALIAS_ADJECTIVES)) % len(_ALIAS_NOUNS)]
    suffix = (value // (len(_ALIAS_ADJECTIVES) * len(_ALIAS_NOUNS))) % 100
    return f"{adjective}{noun}{suffix:02d}"


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


def _temporal_features(session_epochs: list[int]) -> dict:
    """
    Diurnal / calendar fingerprint from an IP's session start epochs (UTC).

    A monitor or scraper runs for months, most days, at all hours; a human analysis project clusters
    into a few weeks, on weekdays, in working hours. These separate presence *pattern* from volume:

      * active_timespan_days   — span from first to last session (a poller runs for months).
      * distinct_active_days   — number of distinct UTC calendar days with any session.
      * presence_density       — distinct_active_days / active_timespan_days (1.0 = present every day).
      * sessions_per_active_day
      * off_hours_fraction     — sessions starting outside 08:00-20:00 UTC (bots don't sleep).
      * weekend_fraction       — sessions on Sat/Sun (cron CI is flat across the week).
    """
    import datetime

    if not session_epochs:
        return {
            "active_timespan_days": 0.0,
            "distinct_active_days": 0,
            "presence_density": 0.0,
            "sessions_per_active_day": 0.0,
            "off_hours_fraction": 0.0,
            "weekend_fraction": 0.0,
        }
    ordered = sorted(session_epochs)
    span_days = (ordered[-1] - ordered[0]) / 86400.0
    days = set()
    off_hours = 0
    weekend = 0
    for epoch in ordered:
        moment = datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc)
        days.add(moment.date())
        if moment.hour < 8 or moment.hour >= 20:
            off_hours += 1
        if moment.weekday() >= 5:
            weekend += 1
    n_active_days = len(days)
    n = len(ordered)
    return {
        "active_timespan_days": span_days,
        "distinct_active_days": n_active_days,
        "presence_density": (n_active_days / span_days) if span_days > 0 else 1.0,
        "sessions_per_active_day": n / n_active_days if n_active_days else 0.0,
        "off_hours_fraction": off_hours / n,
        "weekend_fraction": weekend / n,
    }


def _cv(values: list[float]) -> float:
    """Coefficient of variation (std/mean) of positive gaps between consecutive sorted values."""
    if len(values) < 3:
        return float("nan")
    gaps = np.diff(np.array(sorted(values), dtype=np.float64))
    gaps = gaps[gaps > 0]
    if gaps.size < 2:
        return float("nan")
    mean_gap = float(gaps.mean())
    return float(gaps.std() / mean_gap) if mean_gap > 0 else float("nan")


def ip_features(
    record: dict,
    asset_is_testing: list,
    total_assets: int,
    total_testing_assets: int,
    min_sessions: int,
    session_timeout_in_seconds: int,
) -> dict:
    """
    Turn one IP's accumulated activity into the feature row. ``record`` carries ``sessions`` (a list of
    ``(start, end, asset_index)`` per-(IP,asset) view sessions), ``total_bytes`` and
    ``n_streaming_requests``; ``asset_is_testing`` maps asset_index -> whether it is a testing asset.

    Reports both session models: per-(IP,asset) *view sessions* (the shipped number_of_views unit) and
    IP-level *visits* (8h-gapped across all assets), the latter giving files-per-visit and visit duration.
    """
    sessions = record["sessions"]
    n_sessions = len(sessions)
    starts = [start for start, _end, _asset in sessions]
    durations = [end - start for start, end, _asset in sessions]
    per_asset = collections.Counter(asset for _s, _e, asset in sessions)
    n_assets = len(per_asset)
    distinct_testing = sum(1 for asset in per_asset if asset_is_testing[asset])
    testing_sessions = sum(count for asset, count in per_asset.items() if asset_is_testing[asset])

    cv, dominant, median_gap_h = _timing_features(starts) if n_sessions >= min_sessions else (float("nan"),) * 3
    visits = _visits(sessions, session_timeout_in_seconds)
    n_visits = len(visits)
    visit_files = [v["n_files"] for v in visits]
    new_fractions = [v["n_new"] / v["n_files"] for v in visits if v["n_files"]]
    download_requests = record["n_download_requests"]
    stream_requests = record["n_streaming_requests"]
    temporal = _temporal_features(starts)
    return {
        "n_sessions": n_sessions,
        "n_distinct_assets": n_assets,
        "coverage_fraction": n_assets / total_assets if total_assets else float("nan"),
        "selection_entropy": _selection_entropy(list(per_asset.values())),
        "session_gap_cv": cv,
        "dominant_period_fraction": dominant,
        "median_session_gap_hours": median_gap_h,
        "mean_session_bytes": record["total_bytes"] / n_sessions if n_sessions else 0.0,
        "streaming_requests": stream_requests,
        "download_requests": download_requests,
        # download (200) vs stream (206) mix: tooling that always downloads sits high, genuine
        # streaming sits near 0. NaN when the IP made no download or stream requests at all.
        "download_stream_ratio": (
            download_requests / stream_requests if stream_requests else (float("inf") if download_requests else 0.0)
        ),
        "avg_session_duration_s": (sum(durations) / len(durations)) if durations else 0.0,
        # median vs mean session duration: a big mean/median gap = a few long sessions among many
        # instant ones (probe-then-skip scanning); a tight ratio = uniform behavior.
        "median_session_duration_s": float(np.median(durations)) if durations else 0.0,
        "distinct_testing_assets": distinct_testing,
        "distinct_nontesting_assets": n_assets - distinct_testing,
        "testing_assets_pct": (100 * distinct_testing / total_testing_assets) if total_testing_assets else 0.0,
        # touches ONLY testing assets — a pure-CI/monitoring IP sits True; a dev who also does real
        # analysis sits False (they touch non-testing assets too).
        "testing_only": distinct_testing > 0 and (n_assets - distinct_testing) == 0,
        "n_visits": n_visits,
        "avg_visit_duration_s": (sum(v["duration"] for v in visits) / n_visits) if n_visits else 0.0,
        "avg_files_per_visit": (sum(visit_files) / n_visits) if n_visits else 0.0,
        # mean per-visit fraction of assets never seen in an earlier visit: a scanner marching through
        # the archive is ~1 (all new); a returning analyst re-touches known files, so it is lower.
        "new_asset_fraction_per_visit": (sum(new_fractions) / len(new_fractions)) if new_fractions else 0.0,
        # regularity of the gaps between 8h visits — catches the "every day at the same time" cron
        # cadence that per-session metronomy can miss (low CV = clockwork).
        "visit_gap_cv": _cv([v["start"] for v in visits]) if n_visits >= min_sessions else float("nan"),
        "testing_fraction": testing_sessions / n_sessions if n_sessions else 0.0,
        **temporal,
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
        # ``sessions``: list of (start_epoch, end_epoch, asset_index) per-(IP,asset) view sessions.
        return {"sessions": [], "total_bytes": 0, "n_streaming_requests": 0, "n_download_requests": 0}

    records: dict[str, dict] = collections.defaultdict(_new_record)
    asset_is_testing: list[bool] = []  # indexed by asset_index (assigned per asset below)
    skipped = 0
    for asset_index, asset_dir in enumerate(tqdm.tqdm(asset_dirs, desc="Profiling assets")):
        relative_path = asset_dir.relative_to(extraction_root).as_posix()
        asset_is_testing.append(any(fnmatch.fnmatch(relative_path, glob) for glob in testing_globs))
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
        per_ip_downloads: dict[str, int] = collections.defaultdict(int)
        for timestamp, download, ip, n_bytes in zip(timestamps, downloads, ips, bytes_sent):
            if download != "0":  # a download (200), not a stream (206) — counted, then skipped
                per_ip_downloads[ip] += 1
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
            sessions = _sessions(epochs, session_timeout_in_seconds)
            if not sessions:
                continue
            record = records[ip]
            record["sessions"].extend((start, end, asset_index) for start, end in sessions)
            record["total_bytes"] += per_ip_bytes[ip]
            record["n_streaming_requests"] += per_ip_requests[ip]
            record["n_download_requests"] += per_ip_downloads[ip]
    if skipped:
        print(f"  Skipped {skipped} asset(s) with missing or misaligned files")

    total_testing_assets = sum(asset_is_testing)
    print(f"Resolving {len(records):,} distinct IPs ({total_testing_assets} testing assets present)...")
    key = _ip_hash_key()
    rows = []
    for ip, record in tqdm.tqdm(records.items(), desc="Resolving + featurizing"):
        features = ip_features(
            record,
            asset_is_testing=asset_is_testing,
            total_assets=total_assets,
            total_testing_assets=total_testing_assets,
            min_sessions=min_sessions,
            session_timeout_in_seconds=session_timeout_in_seconds,
        )
        label = resolver.resolve(ip) or ""
        rows.append(
            {
                "ip_hash": hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest(),
                "alias": _alias(hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest()),
                "region_label": label,
                "service": _service_of(label),
                **features,
            }
        )
    return pd.DataFrame(rows)


def report(
    profiles: pd.DataFrame,
    min_sessions: int,
    testing_regular_min: int = 10,
    top_n: int = 25,
    mirror_files_per_visit: float = 500.0,
    min_visits: int = 3,
) -> None:
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

    # --- Third axis: does the IP *regularly* access the reserved testing assets? An incidental
    #     scanner sweep hits the 8 blobs ~once; a monitor polls them repeatedly. Regular testing
    #     access is thus a poller signal orthogonal to coverage/timing. Coarse (only 8 testing blobs)
    #     and expected to overlap the GH-actions monitors the shipped filter already removes. ---
    if "testing_fraction" in active.columns and (active["testing_fraction"] > 0).any():
        testing_sessions = (active["testing_fraction"] * active["n_sessions"]).round().astype(int)
        regular = testing_sessions >= testing_regular_min
        reg_sessions = int(active.loc[regular, "n_sessions"].sum())
        print("\n  testing-asset access (third axis):")
        print(f"    touch testing assets at all:                 {int((testing_sessions > 0).sum()):,} IPs")
        print(
            f"    REGULARLY (>= {testing_regular_min} testing sessions):        {int(regular.sum()):,} IPs, "
            f"{reg_sessions:,} sessions ({100 * reg_sessions / max(total_sessions, 1):.2f}% of all)"
        )
        by_svc = active.loc[regular].groupby("service")["n_sessions"].sum().sort_values(ascending=False)
        for svc, s in by_svc.items():
            print(f"      {svc:>10}: {int(s):>12,} sessions")
        new_mask = regular & ~systematic
        new_sessions = int(active.loc[new_mask, "n_sessions"].sum())
        combined_sessions = int(active.loc[systematic | regular, "n_sessions"].sum())
        print(
            f"    of the {int(regular.sum()):,} regular testing-accessors, {int(new_mask.sum()):,} are NOT already "
            f"flagged by coverage/metronomic —"
        )
        print(
            f"      those add {new_sessions:,} sessions ({100 * new_sessions / max(total_sessions, 1):.2f}% of all) "
            f"beyond coverage/metronomic (its true marginal contribution)"
        )
        print(
            f"    combined (systematic OR regular-testing): {combined_sessions:,} sessions "
            f"({100 * combined_sessions / max(total_sessions, 1):.2f}% of all)"
        )

    # --- Selection axis: the fraction of each visit's assets the IP had never touched before. This is
    #     the direct measure of "does this actor choose assets like a human?" A re-poller/monitor keeps
    #     hitting the same handful (new ~= 0); a human mixes revisits with fresh picks (new in the middle);
    #     an enumeration scanner marches through the archive touching each once (new ~= 1). Unlike coverage
    #     and gap-CV, this separates the low-coverage, irregular-timing monitors those two axes miss. ---
    #
    #     The new-asset fraction ALONE over-counts scanners, because a one-shot casual visitor (a handful of
    #     assets, touched once, never returned) is also "all new". Volume separates them: a bulk mirror takes
    #     thousands of files per visit, a casual visitor a few. So the archetype is read off BOTH axes —
    #     new-asset fraction and files-per-visit (``mirror_files_per_visit``). ---
    #
    #     GUARD: the new-asset fraction is degenerate for an IP with a single visit (everything it saw was
    #     new by definition, so it scores 1.0 while saying nothing), and coarsely quantized for two or three.
    #     Those IPs are excluded here rather than counted as scanners, and the excluded volume is reported so
    #     the size of what the guard removes stays visible.
    if "new_asset_fraction_per_visit" in active.columns:
        enough_visits = active["n_visits"] >= min_visits
        dropped = active.loc[~enough_visits, "n_sessions"].sum()
        print(
            f"\n  (selection axis needs >= {min_visits} visits to be meaningful; "
            f"{int((~enough_visits).sum()):,} of {len(active):,} active IPs excluded, "
            f"holding {int(dropped):,} sessions = {100 * dropped / max(total_sessions, 1):.1f}% of all)"
        )
        selection_set = active[enough_visits]
        new_frac = selection_set["new_asset_fraction_per_visit"]
        files_per_visit = selection_set["avg_files_per_visit"]
        repoller = new_frac <= 0.15
        all_new = new_frac >= 0.85
        bulk = all_new & (files_per_visit >= mirror_files_per_visit)
        one_shot = all_new & (files_per_visit < mirror_files_per_visit)
        mixed = ~repoller & ~all_new
        print(
            "\n  selection archetype by new-asset-fraction-per-visit x files-per-visit "
            f"(mirror cut: >= {mirror_files_per_visit:,g} files/visit):"
        )
        for name, mask in [
            ("re-poller/monitor (new<=15%)", repoller),
            ("mixed/human       (15-85%)", mixed),
            ("bulk mirror       (new>=85%, many files)", bulk),
            ("one-shot casual   (new>=85%, few files)", one_shot),
        ]:
            s = int(selection_set.loc[mask, "n_sessions"].sum())
            testing_ips = int((selection_set.loc[mask, "testing_fraction"] > 0).sum())
            print(
                f"    {name:<41}: {int(mask.sum()):>5,} IPs, {s:>12,} sessions "
                f"({100 * s / max(total_sessions, 1):5.1f}% of all; {testing_ips} touch testing)"
            )

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


def plot_distributions(profiles: pd.DataFrame, min_sessions: int, out_path: pathlib.Path, min_visits: int = 3) -> None:
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

    fig, axes = plt.subplots(2, 2, figsize=(13.0, 10.0))
    axes = axes.ravel()
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

    # (c) selection — new-asset-fraction-per-visit, on a LINEAR 0..1 axis (it is already a fraction). This
    #     is the axis expected to be genuinely multi-modal: a spike near 0 (re-pollers/monitors), a spike
    #     near 1 (scanners/CI), and a human bulk in between. Valleys between those modes are defensible cuts.
    #     The axis is DEGENERATE for an IP with a single visit: everything it touched was new by
    #     definition, so it scores exactly 1.0 while carrying no information. Low visit counts also
    #     quantize it (2 visits can only score 0, 0.5 or 1). Both histograms are drawn — every IP, and
    #     only those with enough visits for the measure to mean anything — so that the share of the
    #     spike at 1.0 that is an artifact is visible rather than assumed.
    ax = axes[3]
    new_frac = active["new_asset_fraction_per_visit"].to_numpy(float)
    new_frac = new_frac[np.isfinite(new_frac)]
    guarded = active[active["n_visits"] >= min_visits]
    guarded_frac = guarded["new_asset_fraction_per_visit"].to_numpy(float)
    guarded_frac = guarded_frac[np.isfinite(guarded_frac)]
    if new_frac.size:
        ax.hist(
            new_frac,
            bins=40,
            range=(0, 1),
            color="mediumpurple",
            alpha=0.35,
            edgecolor="none",
            label=f"all IPs (n={new_frac.size:,})",
        )
    if guarded_frac.size:
        ax.hist(
            guarded_frac,
            bins=40,
            range=(0, 1),
            color="rebeccapurple",
            alpha=0.9,
            edgecolor="none",
            label=f"≥ {min_visits} visits (n={guarded_frac.size:,})",
        )
    ax.axvline(0.15, color="crimson", ls="--", lw=0.9)
    ax.axvline(0.85, color="crimson", ls="--", lw=0.9)
    ax.set_xlabel("new-asset fraction per visit (0 = pure re-poll, 1 = all new)")
    ax.set_ylabel("IPs")
    ax.set_title("(c) selection — does the spike at 1.0 survive the visit guard?", fontsize=9)
    ax.legend(fontsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def plot_selection_plane(
    profiles: pd.DataFrame,
    min_sessions: int,
    mirror_files_per_visit: float,
    out_path: pathlib.Path,
    min_visits: int = 3,
) -> None:
    """
    The two-axis selection plane: new-asset fraction per visit (x) against files per visit (y).

    The new-asset fraction alone cannot separate a bulk mirror from a one-shot casual visitor — both are
    "all new". Volume does: a mirror takes thousands of files per visit, a casual visitor a few. On this
    plane the archetypes occupy distinct corners:

      * right + high  — bulk mirror / enumeration sweep (all new, thousands of files per visit)
      * right + low   — one-shot casual visitor (all new, but only a handful of files)
      * left  (any y) — re-poller / monitor (re-takes the same assets, nothing new)
      * middle        — human working pattern (mixes revisits with fresh picks)

    The companion panel is the files-per-visit distribution, which is what justifies (or refutes) the
    horizontal cut between "mirror" and "casual".
    """
    import matplotlib.pyplot as plt

    active = profiles[profiles["n_sessions"] >= min_sessions].copy()
    if active.empty or "new_asset_fraction_per_visit" not in active.columns:
        print("  (nothing to plot for the selection plane)")
        return
    # The x axis is degenerate below `min_visits` (a single-visit IP scores 1.0 by construction), so those
    # IPs are dropped rather than plotted into the scanner corner as though they had been measured.
    n_before = len(active)
    active = active[active["n_visits"] >= min_visits]
    if active.empty:
        print("  (no IPs survive the visit guard for the selection plane)")
        return

    fig, axes = plt.subplots(1, 2, figsize=(15.0, 6.5), gridspec_kw={"width_ratios": [1.6, 1.0]})
    fig.suptitle(
        f"Selection plane: what an actor chooses, and how much of it "
        f"({len(active):,} IPs ≥ {min_sessions} sessions and ≥ {min_visits} visits; "
        f"{n_before - len(active):,} dropped by the visit guard)",
        fontsize=12,
        fontweight="bold",
    )

    ax = axes[0]
    scatter = ax.scatter(
        active["new_asset_fraction_per_visit"],
        active["avg_files_per_visit"].clip(lower=0.5),
        c=np.log10(active["n_sessions"].clip(lower=1)),
        s=6 + 30 * np.log10(active["n_sessions"].clip(lower=1)),
        cmap="viridis",
        alpha=0.6,
        linewidths=0,
    )
    ax.set_yscale("log")
    ax.axvline(0.15, color="crimson", ls="--", lw=0.9)
    ax.axvline(0.85, color="crimson", ls="--", lw=0.9)
    ax.axhline(mirror_files_per_visit, color="crimson", ls=":", lw=1.0)
    ax.set_xlabel("new-asset fraction per visit  (0 = pure re-poll, 1 = all new)")
    ax.set_ylabel("files per visit (log)")
    ax.set_title("archetype corners: mirror (top-right), casual (bottom-right), monitor (left)", fontsize=9)
    for x, y, text in [
        (0.94, 0.94, "bulk mirror"),
        (0.94, 0.04, "one-shot casual"),
        (0.02, 0.94, "re-poller / monitor"),
        (0.42, 0.94, "human"),
    ]:
        ax.text(x, y, text, transform=ax.transAxes, fontsize=8, ha="right" if x > 0.5 else "left", va="top", alpha=0.75)
    fig.colorbar(scatter, ax=ax, label="log₁₀ sessions")

    ax = axes[1]
    fpv = active["avg_files_per_visit"].to_numpy(float)
    fpv = fpv[np.isfinite(fpv) & (fpv > 0)]
    if fpv.size:
        ax.hist(np.log10(fpv), bins=60, color="steelblue", alpha=0.85, edgecolor="none")
    ax.axvline(
        np.log10(mirror_files_per_visit),
        color="crimson",
        ls=":",
        lw=1.0,
        label=f"{mirror_files_per_visit:,g} files/visit (mirror cut)",
    )
    ax.set_xlabel("files per visit (log₁₀)")
    ax.set_ylabel("IPs")
    ax.set_title("is there a valley separating mirrors from human-scale visits?", fontsize=9)
    ax.legend(fontsize=8)

    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def plot_testing_conditional(
    profiles: pd.DataFrame, min_sessions: int, testing_regular_min: int, out_path: pathlib.Path
) -> None:
    """
    The coverage-vs-CV plane split on the third axis: IPs that *regularly* access the reserved testing
    assets vs the rest. If the regular-testing group occupies a distinct region (e.g. low coverage +
    low CV = pollers), testing access classifies bots the coverage/timing axes miss; if it just
    overlays the scanner/CI cloud, it adds nothing beyond what those axes already flag.
    """
    import matplotlib.pyplot as plt

    active = profiles[(profiles["n_sessions"] >= min_sessions) & profiles["session_gap_cv"].notna()].copy()
    if active.empty or "testing_fraction" not in active.columns:
        print("  (nothing to plot for testing-conditional)")
        return
    testing_sessions = (active["testing_fraction"] * active["n_sessions"]).round()
    active["regular_testing"] = testing_sessions >= testing_regular_min

    fig, axes = plt.subplots(1, 2, figsize=(14.0, 6.0), sharex=True, sharey=True)
    fig.suptitle(
        f"Coverage vs CV, conditioned on regular testing access (≥ {testing_regular_min} testing sessions)",
        fontsize=12,
        fontweight="bold",
    )
    for ax, (title, sub) in zip(
        axes,
        [
            ("regularly accesses testing assets", active[active["regular_testing"]]),
            ("does not", active[~active["regular_testing"]]),
        ],
    ):
        if len(sub):
            ax.scatter(
                sub["coverage_fraction"].clip(lower=1e-6),
                sub["session_gap_cv"].clip(lower=1e-3),
                c=np.log10(sub["n_sessions"].clip(lower=1)),
                s=6 + 30 * np.log10(sub["n_sessions"].clip(lower=1)),
                cmap="viridis",
                alpha=0.6,
                linewidths=0,
            )
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.axhline(0.1, color="crimson", ls="--", lw=0.8)
        ax.axvline(0.02, color="crimson", ls="--", lw=0.8)
        ax.set_xlabel("archive coverage fraction")
        ax.set_title(f"{title}  (n={len(sub):,})", fontsize=9)
    axes[0].set_ylabel("session gap CV")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved {out_path}")


def _fmt_duration(seconds: float) -> str:
    """Human-readable duration from seconds (e.g. '3.2h', '12m', '45s')."""
    if seconds >= 3600:
        return f"{seconds / 3600:.1f}h"
    if seconds >= 60:
        return f"{seconds / 60:.1f}m"
    return f"{seconds:.0f}s"


def behavior_tables(
    profiles: pd.DataFrame, min_sessions: int, out_path: pathlib.Path, top_n: int = 100, min_visits: int = 3
) -> None:
    """
    Emit two derived view-behavior tables and write them as CSVs next to ``out_path``:

    * per source *category* (service label) — an aggregate summary;
    * per source *IP* (top-N by view sessions), with a stable CamelCase alias instead of the raw/hashed IP.

    Columns cover both session models: per-(IP,asset) *view sessions* and IP-level *visits* (files-per-visit,
    visit duration), plus testing vs non-testing distinct-asset counts and the testing-asset percentage.
    """
    active = profiles[profiles["n_sessions"] >= min_sessions].copy()
    if active.empty:
        print("\n(no active IPs for behavior tables)")
        return
    active["view_sessions"] = active["n_sessions"]
    # download/stream ratio is +inf for a pure-downloader (no streams); drop those from the mean so a
    # single pure-downloader doesn't blow the per-service average up to infinity.
    active["download_stream_ratio_finite"] = active["download_stream_ratio"].replace(np.inf, np.nan)
    # The new-asset fraction is degenerate below `min_visits` (a single-visit IP scores 1.0 by
    # construction), so it is averaged over only the IPs where it was actually measurable. Without this
    # the services dominated by one-shot actors (AWS, GCP) report ~99% "all new" purely as an artifact.
    # `n_ips_selection` records how many IPs back the figure, so a thin average is visible as thin.
    active["new_asset_fraction_guarded"] = active["new_asset_fraction_per_visit"].where(
        active["n_visits"] >= min_visits
    )

    # --- Per source category (service) ---
    grouped = active.groupby("service")
    per_service = pd.DataFrame(
        {
            "n_ips": grouped.size(),
            "view_sessions": grouped["view_sessions"].sum().astype(int),
            "visits": grouped["n_visits"].sum().astype(int),
            "avg_session_duration_s": grouped["avg_session_duration_s"].mean(),
            "median_session_duration_s": grouped["median_session_duration_s"].mean(),
            "avg_visit_duration_s": grouped["avg_visit_duration_s"].mean(),
            "avg_files_per_visit": grouped["avg_files_per_visit"].mean(),
            "n_ips_selection": grouped["new_asset_fraction_guarded"].count(),
            "mean_new_asset_fraction_per_visit": grouped["new_asset_fraction_guarded"].mean(),
            "mean_download_stream_ratio": grouped["download_stream_ratio_finite"].mean(),
            "mean_active_timespan_days": grouped["active_timespan_days"].mean(),
            "mean_distinct_active_days": grouped["distinct_active_days"].mean(),
            "mean_presence_density": grouped["presence_density"].mean(),
            "mean_off_hours_fraction": grouped["off_hours_fraction"].mean(),
            "mean_weekend_fraction": grouped["weekend_fraction"].mean(),
            "n_testing_only_ips": grouped["testing_only"].sum().astype(int),
            "mean_distinct_testing_assets": grouped["distinct_testing_assets"].mean(),
            "mean_distinct_nontesting_assets": grouped["distinct_nontesting_assets"].mean(),
            "testing_session_pct": 100
            * (active["testing_fraction"] * active["view_sessions"]).groupby(active["service"]).sum()
            / grouped["view_sessions"].sum(),
        }
    ).sort_values("view_sessions", ascending=False)

    print(f"\n=== View behavior per source category ===  (full columns in the CSV; new% over ≥{min_visits} visits)")
    print(
        f"    {'service':<11}{'IPs':>7}{'views':>12}{'visits':>9}{'avg_sess':>9}{'files/vis':>10}"
        f"{'new%/vis':>9}{'(nIPs)':>8}{'span_d':>8}{'actdays':>8}{'off_hrs%':>9}{'wknd%':>7}{'test%':>7}"
    )
    for service, r in per_service.iterrows():
        new_pct = f"{100 * r.mean_new_asset_fraction_per_visit:>8.1f}%" if r.n_ips_selection else f"{'n/a':>9}"
        print(
            f"    {service:<11}{int(r.n_ips):>7,}{int(r.view_sessions):>12,}{int(r.visits):>9,}"
            f"{_fmt_duration(r.avg_session_duration_s):>9}{r.avg_files_per_visit:>10.1f}"
            f"{new_pct}{int(r.n_ips_selection):>8,}{r.mean_active_timespan_days:>8.0f}"
            f"{r.mean_distinct_active_days:>8.0f}{100 * r.mean_off_hours_fraction:>8.1f}%"
            f"{100 * r.mean_weekend_fraction:>6.1f}%{r.testing_session_pct:>6.1f}%"
        )
    service_csv = out_path.with_name(f"{out_path.stem}_by_service.csv")
    per_service.to_csv(service_csv)
    print(f"  Saved {service_csv}")

    # --- Per source IP (top-N), aliased ---
    cols = [
        "alias",
        "service",
        "region_label",
        "view_sessions",
        "avg_session_duration_s",
        "median_session_duration_s",
        "n_visits",
        "avg_visit_duration_s",
        "avg_files_per_visit",
        "new_asset_fraction_per_visit",
        "download_stream_ratio",
        "distinct_testing_assets",
        "testing_assets_pct",
        "distinct_nontesting_assets",
        "testing_only",
        "coverage_fraction",
        "session_gap_cv",
        "visit_gap_cv",
        "active_timespan_days",
        "distinct_active_days",
        "presence_density",
        "off_hours_fraction",
        "weekend_fraction",
    ]
    top = active.nlargest(top_n, "view_sessions")[cols].copy()
    print(f"\n=== View behavior per source IP (top {min(top_n, len(top))} by view sessions) ===  (full columns in CSV)")
    print(
        f"    {'alias':<16}{'label':<13}{'views':>9}{'f/vis':>7}{'new%':>6}{'span_d':>8}{'actdays':>8}"
        f"{'pres':>6}{'off%':>6}{'wknd%':>6}{'cov':>7}{'sCV':>6}{'vCV':>6}{'test':>6}"
    )
    for r in top.itertuples(index=False):
        print(
            f"    {r.alias:<16}{(r.region_label or '?'):<13}{int(r.view_sessions):>9,}"
            f"{r.avg_files_per_visit:>7.1f}{100 * r.new_asset_fraction_per_visit:>5.0f}%"
            f"{r.active_timespan_days:>8.0f}{int(r.distinct_active_days):>8,}{r.presence_density:>6.2f}"
            f"{100 * r.off_hours_fraction:>5.0f}%{100 * r.weekend_fraction:>5.0f}%"
            f"{r.coverage_fraction:>7.4f}{r.session_gap_cv:>6.2f}{r.visit_gap_cv:>6.2f}"
            f"{int(r.distinct_testing_assets):>6}"
        )
    ip_csv = out_path.with_name(f"{out_path.stem}_by_ip.csv")
    top.to_csv(ip_csv, index=False)
    print(f"  Saved {ip_csv}")


def _cache_paths(cache_dir: pathlib.Path) -> tuple[pathlib.Path, pathlib.Path]:
    base = cache_dir / "analysis_cache"
    # _v3: schema gained the temporal fingerprint (active timespan, active days, presence density,
    # off-hours / weekend fractions), visit-gap CV, download/stream mix, per-visit new-asset fraction,
    # median session duration, and the testing-only flag — an older cache lacks these, so it is not reused.
    return base / "ip_behavior_profiles_v3.parquet", base / "ip_behavior_profiles_v3.csv.gz"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--testing-asset-file", type=pathlib.Path, default=None)
    parser.add_argument("--min-sessions", type=int, default=20, help="Timing-score IPs with >= this many sessions")
    parser.add_argument(
        "--testing-regular-min",
        type=int,
        default=10,
        help="An IP with >= this many testing-asset sessions is a 'regular' testing accessor (a poller), "
        "distinct from an archive scanner that touches the testing blobs once incidentally.",
    )
    parser.add_argument(
        "--mirror-files-per-visit",
        type=float,
        default=500.0,
        help="An all-new IP taking >= this many files per visit is a bulk mirror rather than a one-shot casual "
        "visitor. Tune it against the files-per-visit panel of the selection-plane plot.",
    )
    parser.add_argument(
        "--min-visits",
        type=int,
        default=3,
        help="The selection axis (new-asset fraction) is degenerate below this many visits -- a single-visit "
        "IP scores 1.0 by construction -- so such IPs are excluded from it.",
    )
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

    report(
        profiles,
        min_sessions=args.min_sessions,
        testing_regular_min=args.testing_regular_min,
        mirror_files_per_visit=args.mirror_files_per_visit,
        min_visits=args.min_visits,
    )
    behavior_tables(profiles, min_sessions=args.min_sessions, out_path=args.out, min_visits=args.min_visits)
    try:
        plot(profiles, min_sessions=args.min_sessions, out_path=args.out)
        distributions_path = args.out.with_name(f"{args.out.stem}_distributions{args.out.suffix}")
        plot_distributions(
            profiles, min_sessions=args.min_sessions, out_path=distributions_path, min_visits=args.min_visits
        )
        selection_path = args.out.with_name(f"{args.out.stem}_selection_plane{args.out.suffix}")
        plot_selection_plane(
            profiles,
            min_sessions=args.min_sessions,
            mirror_files_per_visit=args.mirror_files_per_visit,
            out_path=selection_path,
            min_visits=args.min_visits,
        )
        testing_path = args.out.with_name(f"{args.out.stem}_testing_conditional{args.out.suffix}")
        plot_testing_conditional(
            profiles,
            min_sessions=args.min_sessions,
            testing_regular_min=args.testing_regular_min,
            out_path=testing_path,
        )
    except ImportError:
        print("  (matplotlib not installed — skipped the plots)")


if __name__ == "__main__":
    main()

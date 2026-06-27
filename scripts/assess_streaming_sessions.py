"""
Session separability assessment for streaming requests.

Loads all streaming (HTTP 206, download=0) requests from the extraction cache,
computes inter-request intervals per IP, and produces two plots:

1. Empirical CDF + histogram of inter-request intervals on a log time axis
   (bin-size-agnostic — reveals natural bimodality if present).

2. For each candidate session bin size (10 min, 30 min, 1 hr, 2 hr, 4 hr),
   the distribution of "gap-to-next-request-beyond-the-bin" across all IPs/assets.
   A clean gap distribution (mass concentrated at large values) supports using
   that bin as a session timeout.

Usage
-----
    python assess_streaming_sessions.py --cache-dir /path/to/extraction/cache \
        [--dataset DANDI:000123] [--no-encryption] [--out session_assessment.png]

The script requires: numpy, pandas, matplotlib, tqdm.
If IPs are encrypted, the encryption password must be available via the
S3_LOG_EXTRACTION_PASS environment variable (same as the main library).
"""

import argparse
import pathlib
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tqdm

TIMESTAMP_FORMAT = "%y%m%d%H%M%S"  # YYMMDDHHmmss


def _parse_timestamp(ts_str: str) -> pd.Timestamp:
    return pd.to_datetime(ts_str.strip(), format=TIMESTAMP_FORMAT)


def _read_lines(path: pathlib.Path) -> list[str]:
    return [line.strip() for line in path.read_text().splitlines() if line.strip()]


def _read_ips(path: pathlib.Path, use_encryption: bool) -> list[str]:
    if not use_encryption:
        return _read_lines(path)
    # Reuse the library's decryption helper
    sys.path.insert(0, str(pathlib.Path(__file__).parent / "src"))
    from s3_log_extraction.utils.encryption import read_text_from_file

    text = read_text_from_file(file_path=path, use_encryption=True)
    return [line.strip() for line in text.splitlines() if line.strip()]


def load_streaming_requests(
    cache_dir: pathlib.Path,
    dataset_filter: str | None,
    use_encryption: bool,
) -> pd.DataFrame:
    """Walk the extraction cache and collect all streaming (download=0) requests."""
    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory found under {cache_dir}")

    # Gather all asset directories (leaf dirs containing timestamps.txt)
    asset_dirs = []
    for dataset_dir in sorted(extraction_root.iterdir()):
        if not dataset_dir.is_dir():
            continue
        if dataset_filter and dataset_filter not in dataset_dir.name:
            continue
        for asset_dir in dataset_dir.rglob("*"):
            if (asset_dir / "timestamps.txt").exists():
                asset_dirs.append(asset_dir)

    if not asset_dirs:
        raise ValueError(f"No asset directories with timestamps.txt found under {extraction_root}")

    print(f"Found {len(asset_dirs)} asset directories")

    records = []
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Loading assets"):
        timestamps_path = asset_dir / "timestamps.txt"
        downloads_path = asset_dir / "download.txt"
        ips_path = asset_dir / "ips.txt"

        if not (downloads_path.exists() and ips_path.exists()):
            continue

        timestamps_raw = _read_lines(timestamps_path)
        downloads_raw = _read_lines(downloads_path)
        ips_raw = _read_ips(ips_path, use_encryption)

        n = len(timestamps_raw)
        if not (len(downloads_raw) == n == len(ips_raw)):
            print(f"  Warning: mismatched line counts in {asset_dir}, skipping")
            continue

        for ts_str, dl_str, ip in zip(timestamps_raw, downloads_raw, ips_raw):
            if dl_str == "0":  # streaming only
                try:
                    ts = _parse_timestamp(ts_str)
                    records.append({"timestamp": ts, "ip": ip})
                except ValueError:
                    pass

    if not records:
        raise ValueError("No streaming requests found — check cache path and filters")

    df = pd.DataFrame(records)
    df.sort_values("timestamp", inplace=True)
    print(f"Loaded {len(df):,} streaming requests from {df['ip'].nunique():,} unique IPs")
    return df


def compute_inter_request_intervals(df: pd.DataFrame) -> np.ndarray:
    """Return array of inter-request intervals in seconds, computed per IP."""
    intervals = []
    groups = list(df.groupby("ip"))
    for _ip, group in tqdm.tqdm(groups, desc="Computing intervals", unit="IP"):
        times = group["timestamp"].sort_values()
        deltas = times.diff().dropna().dt.total_seconds()
        intervals.extend(deltas[deltas > 0].tolist())
    return np.array(intervals)


def compute_gap_beyond_bin(df: pd.DataFrame, bin_seconds: int, label: str = "") -> np.ndarray:
    """
    For each IP, find requests that would be the *last* in a time bin,
    then measure how long until the *next* request from that IP.
    Returns array of those gap durations in seconds (NaN = no next request).
    """
    gaps = []
    groups = list(df.groupby("ip"))
    for _ip, group in tqdm.tqdm(groups, desc=f"Gap-beyond-bin ({label})", unit="IP"):
        times = group["timestamp"].sort_values().reset_index(drop=True)
        ts_sec = times.astype(np.int64) // 10**9  # epoch seconds

        i = 0
        while i < len(ts_sec):
            bin_start = ts_sec.iloc[i]
            bin_end = bin_start + bin_seconds
            # find last request within this bin
            in_bin = ts_sec[ts_sec < bin_end]
            last_in_bin_idx = in_bin[in_bin.index >= i].index.max() if not in_bin[in_bin.index >= i].empty else i
            # find next request after the bin
            after_bin = ts_sec[ts_sec >= bin_end]
            if after_bin.empty:
                break
            next_after_bin_idx = after_bin.index.min()
            gap = ts_sec.iloc[next_after_bin_idx] - ts_sec.iloc[last_in_bin_idx]
            gaps.append(float(gap))
            i = next_after_bin_idx

    return np.array(gaps)


def _fmt_seconds(s: float) -> str:
    """Human-readable duration."""
    if s < 90:
        return f"{s:.0f}s"
    if s < 5400:
        return f"{s / 60:.1f}m"
    if s < 129600:
        return f"{s / 3600:.1f}h"
    return f"{s / 86400:.2f}d"


def find_emptiest_band(intervals: np.ndarray, lo_seconds: float = 60.0, hi_seconds: float = 7 * 86400.0) -> dict | None:
    """
    Find the widest contiguous band of lag values containing ZERO observed
    inter-request intervals, restricted to ``[lo_seconds, hi_seconds]``.

    A truly empty band is the deterministic "moat": placing the session timeout
    anywhere inside it means no observed gap sits near the boundary, so no request
    is ambiguously classified. Returns the widest such band measured two ways —
    by absolute width and by log10 ratio (more meaningful on a log time axis) —
    each with a suggested timeout ``T`` at the geometric midpoint.

    Returns ``None`` if fewer than two intervals fall in the region of interest.
    """
    region = np.sort(intervals[(intervals >= lo_seconds) & (intervals <= hi_seconds)])
    if region.size < 2:
        return None

    lower = region[:-1]
    upper = region[1:]
    abs_width = upper - lower
    log_ratio = np.log10(upper) - np.log10(lower)

    def _band(i: int) -> dict:
        a, b = float(region[i]), float(region[i + 1])
        return {
            "lower": a,
            "upper": b,
            "width_seconds": b - a,
            "log10_ratio": float(np.log10(b) - np.log10(a)),
            "geometric_mid": float(np.sqrt(a * b)),
            "arithmetic_mid": 0.5 * (a + b),
        }

    return {
        "widest_absolute": _band(int(np.argmax(abs_width))),
        "widest_logratio": _band(int(np.argmax(log_ratio))),
    }


def guard_band_report(intervals: np.ndarray, candidate_seconds: float, rel_tolerance: float = 0.1) -> dict:
    """
    For a candidate session timeout ``T``, count observed intervals falling within
    the multiplicative guard band ``[T / (1 + tol), T * (1 + tol)]`` — the
    "ambiguity count". Zero means the cutoff is perfectly deterministic in this
    snapshot; this same count is the metric to monitor on future data.
    """
    lo = candidate_seconds / (1 + rel_tolerance)
    hi = candidate_seconds * (1 + rel_tolerance)
    in_band = int(np.count_nonzero((intervals >= lo) & (intervals <= hi)))
    return {
        "candidate_seconds": candidate_seconds,
        "guard_lo": lo,
        "guard_hi": hi,
        "ambiguity_count": in_band,
        "ambiguity_fraction": in_band / intervals.size if intervals.size else 0.0,
    }


def plot_assessment(
    intervals: np.ndarray,
    bin_configs: list[tuple[str, int]],
    df: pd.DataFrame,
    out_path: pathlib.Path,
) -> None:
    n_bins = len(bin_configs)
    fig, axes = plt.subplots(2, n_bins + 1, figsize=(5 * (n_bins + 1), 10))
    fig.suptitle("Streaming Session Separability Assessment", fontsize=14, fontweight="bold")

    # --- Row 0, Col 0: inter-request interval histogram (log x) ---
    ax = axes[0, 0]
    log_intervals = np.log10(intervals[intervals > 0])
    ax.hist(log_intervals, bins=100, color="steelblue", edgecolor="none", alpha=0.8)
    ax.set_xlabel("Inter-request interval (seconds, log₁₀ scale)")
    ax.set_ylabel("Count")
    ax.set_title("Histogram of inter-request intervals\n(all IPs, streaming only)")
    tick_vals = [1, 60, 600, 3600, 86400, 86400 * 7]
    ax.set_xticks([np.log10(v) for v in tick_vals])
    ax.set_xticklabels(["1s", "1m", "10m", "1h", "1d", "1w"], fontsize=8)

    # --- Row 1, Col 0: empirical CDF ---
    ax = axes[1, 0]
    sorted_intervals = np.sort(intervals[intervals > 0])
    cdf = np.arange(1, len(sorted_intervals) + 1) / len(sorted_intervals)
    ax.semilogx(sorted_intervals, cdf, color="steelblue", linewidth=1.5)
    ax.set_xlabel("Inter-request interval (seconds, log scale)")
    ax.set_ylabel("Cumulative fraction")
    ax.set_title("Empirical CDF of inter-request intervals")
    ax.grid(True, alpha=0.3)
    for label, secs in [("10m", 600), ("30m", 1800), ("1h", 3600), ("2h", 7200), ("4h", 14400)]:
        ax.axvline(secs, color="tomato", linestyle="--", linewidth=0.8, alpha=0.7)
        frac = np.searchsorted(sorted_intervals, secs) / len(sorted_intervals)
        ax.text(secs * 1.05, frac, f"{label}\n{frac:.1%}", fontsize=7, color="tomato", va="center")

    # --- Columns 1..N: gap-beyond-bin plots ---
    for col_idx, (label, bin_sec) in enumerate(bin_configs):
        gaps = compute_gap_beyond_bin(df, bin_sec, label=label)
        gaps = gaps[np.isfinite(gaps) & (gaps > 0)]

        # Row 0: histogram
        ax = axes[0, col_idx + 1]
        if len(gaps):
            log_gaps = np.log10(gaps)
            ax.hist(log_gaps, bins=60, color="darkorange", edgecolor="none", alpha=0.8)
        ax.set_xlabel("Gap to next request beyond bin (s, log₁₀)")
        ax.set_ylabel("Count")
        ax.set_title(f"Gap-beyond-bin: {label} window\n(n={len(gaps):,} bin boundaries)")
        tick_vals = [1, 60, 600, 3600, 86400]
        ax.set_xticks([np.log10(v) for v in tick_vals])
        ax.set_xticklabels(["1s", "1m", "10m", "1h", "1d"], fontsize=8)
        ax.axvline(np.log10(bin_sec), color="steelblue", linestyle="--", linewidth=1, label="bin size")
        ax.legend(fontsize=7)

        # Row 1: CDF
        ax = axes[1, col_idx + 1]
        if len(gaps):
            sorted_gaps = np.sort(gaps)
            cdf = np.arange(1, len(sorted_gaps) + 1) / len(sorted_gaps)
            ax.semilogx(sorted_gaps, cdf, color="darkorange", linewidth=1.5)
            ax.axvline(bin_sec, color="steelblue", linestyle="--", linewidth=1, label="bin size")
            ax.legend(fontsize=7)
        ax.set_xlabel("Gap to next request (seconds, log scale)")
        ax.set_ylabel("Cumulative fraction")
        ax.set_title(f"CDF of gap-beyond-bin: {label}")
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    print(f"Saved assessment plot to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cache-dir", required=True, type=pathlib.Path, help="Path to the root extraction cache directory"
    )
    parser.add_argument(
        "--dataset", default=None, help="Optional dataset name/ID substring to filter (e.g. 'DANDI:000123')"
    )
    parser.add_argument("--no-encryption", action="store_true", help="Treat ips.txt files as plaintext (no decryption)")
    parser.add_argument(
        "--out",
        default="session_assessment.png",
        type=pathlib.Path,
        help="Output PNG path (default: session_assessment.png)",
    )
    args = parser.parse_args()

    use_encryption = not args.no_encryption

    df = load_streaming_requests(
        cache_dir=args.cache_dir,
        dataset_filter=args.dataset,
        use_encryption=use_encryption,
    )

    print("Computing inter-request intervals...")
    intervals = compute_inter_request_intervals(df)
    print(f"  {len(intervals):,} intervals computed")

    bin_configs = [
        ("10 min", 10 * 60),
        ("30 min", 30 * 60),
        ("1 hour", 60 * 60),
        ("2 hours", 2 * 60 * 60),
    ]

    print("Computing gap-beyond-bin for each window size...")
    plot_assessment(intervals=intervals, bin_configs=bin_configs, df=df, out_path=args.out)

    # Print summary statistics
    print("\n--- Inter-request interval summary ---")
    for label, secs in bin_configs:
        pct = 100 * np.mean(intervals <= secs)
        print(f"  Fraction of intervals ≤ {label}: {pct:.1f}%")
    print(f"  Median interval: {np.median(intervals):.0f}s")
    print(f"  95th percentile: {np.percentile(intervals, 95):.0f}s")
    print(f"  99th percentile: {np.percentile(intervals, 99):.0f}s")

    # --- Session-boundary determinism diagnostic ---
    # (1) Auto-pick the widest zero-density "moat" between sessions.
    print("\n--- Emptiest (zero-density) band in inter-request interval distribution ---")
    bands = find_emptiest_band(intervals, lo_seconds=60.0, hi_seconds=7 * 86400.0)
    if bands is None:
        print("  Not enough intervals in the 60s–7d region of interest.")
    else:
        for key, title in [("widest_absolute", "widest by absolute width"), ("widest_logratio", "widest by log ratio")]:
            b = bands[key]
            print(
                f"  [{title}] empty band {_fmt_seconds(b['lower'])} – {_fmt_seconds(b['upper'])} "
                f"(width {_fmt_seconds(b['width_seconds'])}, {b['log10_ratio']:.2f} decades); "
                f"suggested T = {_fmt_seconds(b['geometric_mid'])}"
            )

    # (2) Quantify the ambiguity at fixed candidate timeouts, including the 1-day choice.
    print("\n--- Guard-band ambiguity at candidate timeouts (±10% multiplicative window) ---")
    candidates = [
        ("10 min", 600.0),
        ("30 min", 1800.0),
        ("1 hour", 3600.0),
        ("2 hours", 7200.0),
        ("1 day", 86400.0),
    ]
    if bands is not None:
        t_auto = bands["widest_logratio"]["geometric_mid"]
        candidates.append((f"auto-trough ({_fmt_seconds(t_auto)})", t_auto))
    for label, secs in candidates:
        r = guard_band_report(intervals, secs, rel_tolerance=0.1)
        print(
            f"  {label:>24}: {r['ambiguity_count']:>9,} intervals in "
            f"[{_fmt_seconds(r['guard_lo'])}, {_fmt_seconds(r['guard_hi'])}] "
            f"({100 * r['ambiguity_fraction']:.4f}% of all) "
            f"{'← DETERMINISTIC (empty)' if r['ambiguity_count'] == 0 else ''}"
        )


if __name__ == "__main__":
    main()

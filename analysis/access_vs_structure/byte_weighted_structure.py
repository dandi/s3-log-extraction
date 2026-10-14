"""
Byte-weighted structure metrics for NWB (HDF5) files, and the figures to judge them.

Every structural metric in ``WHITEPAPER.md`` (groups, datasets, cophenetic index, out-degrees) treats a
file as an unlabeled tree and so collapses onto object count. This script weights the tree by where the
bytes actually live, to test whether that separates "structure" from "size".

Per HDF5 dataset it records the path, top-level NWB section, shape, itemsize, logical bytes, allocated
storage bytes (``get_storage_size()``, what a reader transfers), layout, chunk shape and count, and
compression filter. Per file, from the storage-byte shares ``p_i`` over datasets with nonzero storage:

    D0 = number of nonzero datasets     D1 = exp(-sum p ln p)     D2 = 1 / sum p**2
    evenness E = D1 / D0                top-1 and top-5 byte shares
    byte shares per top-level section (and a finer table splitting processing modules and general)

The logical-byte versions of D1 and evenness are kept as secondary columns.

Subcommands
-----------
    build   Walk a stratified sample of the files in access_structure.csv over S3 with remfile + h5py,
            caching one parquet of per-dataset rows per file so the walk is resumable, then write the
            per-file table (byte_weighted.csv) and the finer section table.
    plot    Render the figures into figures/byte_weighted/ and print the headline statistics.

Usage
-----
    pip install h5py remfile pandas numpy scipy matplotlib pyarrow tqdm
    python byte_weighted_structure.py build --workers 16 --max-per-dandiset 10 --seed 0
    python byte_weighted_structure.py plot

HDF5 only. Zarr assets are a follow-up.
"""

import argparse
import concurrent.futures
import json
import os
import pathlib
import random
import subprocess
import sys

import numpy as np
import pandas as pd
import tqdm

HERE = pathlib.Path(__file__).parent
S3_URL = "https://dandiarchive.s3.amazonaws.com/blobs/{a}/{b}/{c}"
SECTIONS = (
    "acquisition",
    "processing",
    "analysis",
    "stimulus",
    "intervals",
    "units",
    "general",
    "scratch",
    "specifications",
    "other",
)
JOIN_COLUMNS = [
    "content_id",
    "dandiset_id",
    "asset_path",
    "groups",
    "datasets",
    "cophenetic_index",
    "size_bytes",
    "number_of_requests",
    "number_of_downloads",
]
# Counting chunks walks the whole chunk index a second time, which on a file of hundreds of gigabytes is
# enough to time out. The retry pass for such files sets this so they still contribute storage bytes.
_SKIP_CHUNK_COUNT = os.environ.get("BYTE_WEIGHTED_SKIP_CHUNK_COUNT") == "1"
_LAYOUTS = {0: "compact", 1: "contiguous", 2: "chunked", 3: "virtual"}


# ----------------------------------------------------------------------------------------------- build


def _sections_of(path: str) -> tuple[str, str]:
    """Top-level section, and the finer label that splits processing by module and general by subgroup."""
    parts = path.strip("/").split("/")
    section = parts[0] if parts[0] in SECTIONS and len(parts) > 1 else "other"
    if section in ("processing", "general") and len(parts) > 2:
        return section, f"{section}/{parts[1]}"
    return section, section


def walk_file(content_id: str) -> dict:
    """Open one blob over HTTP and return its per-dataset rows, or the error that stopped it."""
    import h5py
    import remfile

    url = S3_URL.format(a=content_id[:3], b=content_id[3:6], c=content_id)
    rows: list[dict] = []
    seen: set = set()
    try:
        with h5py.File(remfile.File(url), "r") as file:

            def visit(name: str, obj) -> None:
                if not isinstance(obj, h5py.Dataset):
                    return
                # One object reachable by several paths is counted once, by its HDF5 address
                info = h5py.h5o.get_info(obj.id)
                address = getattr(info, "token", None) or getattr(info, "addr", None)
                key = bytes(address) if isinstance(address, (bytes, bytearray, memoryview)) else address
                if key in seen:
                    return
                seen.add(key)

                plist = obj.id.get_create_plist()
                layout = _LAYOUTS.get(plist.get_layout(), "unknown")
                shape = obj.shape or ()
                itemsize = obj.dtype.itemsize
                n_chunks = None
                if layout == "chunked" and not _SKIP_CHUNK_COUNT:
                    try:
                        n_chunks = obj.id.get_num_chunks()
                    except Exception:  # noqa: BLE001 - older HDF5 builds lack the chunk query
                        n_chunks = None
                section, subsection = _sections_of(f"/{name}")
                rows.append(
                    {
                        "content_id": content_id,
                        "path": f"/{name}",
                        "section": section,
                        "subsection": subsection,
                        "shape": json.dumps(list(shape)),
                        "itemsize": itemsize,
                        "logical_bytes": int(np.prod(shape, dtype=np.float64) * itemsize) if shape else itemsize,
                        "storage_bytes": int(obj.id.get_storage_size()),
                        "layout": layout,
                        "chunks": json.dumps(list(obj.chunks)) if obj.chunks else "",
                        "n_chunks": n_chunks,
                        "compression": obj.compression or "",
                    }
                )

            file.visititems(visit)
    except Exception as exception:  # noqa: BLE001 - every failure is recorded and reported, not raised
        return {"content_id": content_id, "error": f"{type(exception).__name__}: {exception}"[:300], "rows": []}
    return {"content_id": content_id, "error": None, "rows": rows}


def stratified_sample(table: pd.DataFrame, max_per_dandiset: int | None, seed: int) -> list[str]:
    """At most ``max_per_dandiset`` files per dandiset, chosen at random with a fixed seed."""
    rng = random.Random(seed)
    chosen: list[str] = []
    for _, group in sorted(table.groupby("dandiset_id"), key=lambda item: item[0]):
        ids = sorted(group["content_id"])
        if max_per_dandiset is not None and len(ids) > max_per_dandiset:
            ids = rng.sample(ids, max_per_dandiset)
        chosen.extend(ids)
    return chosen


def hill_metrics(storage: np.ndarray) -> dict:
    """
    Hill numbers, their successive ratios, and top shares over the nonzero entries of one file's byte vector.

    Hill numbers never increase with their order, D0 >= D1 >= D2 >= D_inf, so the profile is carried exactly by
    one scale and the successive ratios r1 = D1/D0, r2 = D2/D1 and r* = D_inf/D2, each in (0, 1], since
    D_inf = D0 * r1 * r2 * r*. r1 is the evenness. r2 falls when a long tail of mid-sized datasets holds real
    mass, and r* falls when even the leading datasets are unequal. D_inf is 1 / (largest share).
    """
    nonzero = storage[storage > 0].astype(np.float64)
    if nonzero.size == 0:
        empty = {"D0": 0, "D1": np.nan, "D2": np.nan, "D_inf": np.nan, "evenness": np.nan}
        empty.update({"r1": np.nan, "r2": np.nan, "r_star": np.nan, "top1_share": np.nan, "top5_share": np.nan})
        return empty
    shares = nonzero / nonzero.sum()
    d1 = float(np.exp(-np.sum(shares * np.log(shares))))
    d2 = float(1.0 / np.sum(shares**2))
    ordered = np.sort(shares)[::-1]
    d_inf = float(1.0 / ordered[0])
    metrics = {
        "D0": int(nonzero.size),
        "D1": d1,
        "D2": d2,
        "D_inf": d_inf,
        "evenness": d1 / nonzero.size,
        "r1": d1 / nonzero.size,
        "r2": d2 / d1,
        "r_star": d_inf / d2,
        "top1_share": float(ordered[0]),
        "top5_share": float(ordered[:5].sum()),
    }
    return metrics


def per_file_table(datasets: pd.DataFrame, access: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse per-dataset rows to the per-file metrics, plus the finer section-share table."""
    records = []
    for content_id, group in datasets.groupby("content_id"):
        storage = group["storage_bytes"].to_numpy()
        logical = group["logical_bytes"].to_numpy()
        record = {
            "content_id": content_id,
            "n_datasets_walked": len(group),
            "n_zero_storage": int((storage == 0).sum()),
        }
        record.update(hill_metrics(storage))
        logical_metrics = hill_metrics(logical)
        record["D1_logical"] = logical_metrics["D1"]
        record["evenness_logical"] = logical_metrics["evenness"]
        record["total_storage_bytes"] = int(storage.sum())
        record["total_logical_bytes"] = int(logical.sum())
        total = storage.sum()
        by_section = group.groupby("section")["storage_bytes"].sum()
        for section in SECTIONS:
            record[f"share_{section}"] = float(by_section.get(section, 0) / total) if total > 0 else np.nan
        records.append(record)
    per_file = pd.DataFrame(records)
    per_file = access[JOIN_COLUMNS].merge(per_file, on="content_id", how="inner")

    fine = datasets.groupby(["content_id", "subsection"], as_index=False)["storage_bytes"].sum()
    totals = fine.groupby("content_id")["storage_bytes"].transform("sum")
    fine["share"] = np.where(totals > 0, fine["storage_bytes"] / totals.where(totals > 0, 1), np.nan)
    fine = access[["content_id", "dandiset_id"]].merge(fine, on="content_id", how="inner")
    return per_file, fine


def build(args: argparse.Namespace) -> None:
    access = pd.read_csv(args.data)
    sample = stratified_sample(access, args.max_per_dandiset, args.seed)
    cache = args.cache_dir
    (cache / "files").mkdir(parents=True, exist_ok=True)
    failures_path = cache / "failures.jsonl"
    failed_before = set()
    if failures_path.exists():
        failed_before = {json.loads(line)["content_id"] for line in failures_path.read_text().splitlines() if line}
    todo = [
        c
        for c in sample
        if not (cache / "files" / f"{c}.parquet").exists() and (args.retry_failed or c not in failed_before)
    ]
    print(
        f"Sample: {len(sample):,} files from {access['dandiset_id'].nunique()} dandisets "
        f"(max {args.max_per_dandiset} per dandiset, seed {args.seed}); {len(todo):,} still to walk"
    )

    # One subprocess per file: a file that exhausts memory or hangs on a slow chunk index fails alone,
    # instead of breaking a shared process pool and stalling every other worker
    environment = {**os.environ, "BYTE_WEIGHTED_SKIP_CHUNK_COUNT": "1" if args.skip_chunk_count else "0"}

    def run_one(content_id: str) -> tuple[str, str | None]:
        command = [sys.executable, "-I", str(pathlib.Path(__file__).resolve()), "_walk_one", content_id, str(cache)]
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, timeout=args.timeout, check=False, env=environment
            )
        except subprocess.TimeoutExpired:
            return content_id, f"timeout after {args.timeout}s"
        if completed.returncode != 0:
            detail = (completed.stderr.strip().splitlines() or [""])[-1][:200]
            return content_id, f"worker exited with code {completed.returncode} (killed or crashed) {detail}".strip()
        error = completed.stdout.strip()
        return content_id, error or None

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool, failures_path.open("a") as failures:
        futures = [pool.submit(run_one, c) for c in todo]
        for future in tqdm.tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Walking files"):
            content_id, error = future.result()
            if error is not None:
                failures.write(json.dumps({"content_id": content_id, "error": error}) + "\n")
                failures.flush()

    walked = [c for c in sample if (cache / "files" / f"{c}.parquet").exists()]
    datasets = pd.concat([pd.read_parquet(cache / "files" / f"{c}.parquet") for c in walked], ignore_index=True)
    datasets = datasets.dropna(subset=["path"])
    datasets.to_parquet(cache / "datasets.parquet", index=False)

    failed = {}
    if failures_path.exists():
        for line in failures_path.read_text().splitlines():
            entry = json.loads(line)
            if entry["content_id"] in set(sample) and entry["content_id"] not in set(walked):
                failed[entry["content_id"]] = entry["error"]
    pd.DataFrame(sorted(failed.items()), columns=["content_id", "error"]).to_csv(
        HERE / "byte_weighted_failures.csv", index=False
    )

    per_file, fine = per_file_table(datasets, access)
    per_file.to_csv(HERE / "byte_weighted.csv", index=False)
    fine.to_csv(HERE / "byte_weighted_sections_fine.csv", index=False)
    print(f"Walked {len(walked):,} of {len(sample):,} sampled files; {len(failed):,} failed to open")
    print(f"Per-file metrics for {len(per_file):,} files across {per_file['dandiset_id'].nunique()} dandisets")


# ------------------------------------------------------------------------------------------------ plot


def partial_spearman(x: np.ndarray, y: np.ndarray, z: np.ndarray) -> float:
    """Spearman of x and y controlling for z: Pearson of the rank residuals after regressing each on rank z."""
    from scipy import stats

    rx, ry, rz = (stats.rankdata(v) for v in (x, y, z))
    design = np.column_stack([np.ones_like(rz), rz])
    res_x = rx - design @ np.linalg.lstsq(design, rx, rcond=None)[0]
    res_y = ry - design @ np.linalg.lstsq(design, ry, rcond=None)[0]
    value = float(np.corrcoef(res_x, res_y)[0, 1])
    return value


def icc1(values: np.ndarray, groups: np.ndarray) -> float:
    """One-way random-effects ICC(1) with the unbalanced-group size correction."""
    frame = pd.DataFrame({"v": values, "g": groups}).dropna()
    grouped = frame.groupby("g")["v"]
    n_i = grouped.size().to_numpy()
    k, n = len(n_i), len(frame)
    if k < 2 or n <= k:
        return float("nan")
    grand = frame["v"].mean()
    ss_between = float(np.sum(n_i * (grouped.mean().to_numpy() - grand) ** 2))
    ss_within = float(((frame["v"] - grouped.transform("mean")) ** 2).sum())
    ms_between = ss_between / (k - 1)
    ms_within = ss_within / (n - k)
    n0 = (n - np.sum(n_i**2) / n) / (k - 1)
    value = (ms_between - ms_within) / (ms_between + (n0 - 1) * ms_within)
    return float(value)


def plot(args: argparse.Namespace) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from scipy import stats

    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(args.metrics)
    df = df[(df["D0"] > 0) & (df["total_storage_bytes"] > 0)].copy()
    df["log_D0"] = np.log10(df["D0"])
    df["log_bytes"] = np.log10(df["total_storage_bytes"])
    df["log_D1"] = np.log10(df["D1"])
    df["log_D2"] = np.log10(df["D2"])
    df["log_cophenetic"] = np.log10(df["cophenetic_index"].clip(lower=1))
    by_dandiset = df.groupby("dandiset_id").median(numeric_only=True)
    report: dict = {"files": len(df), "dandisets": int(df["dandiset_id"].nunique())}

    # 1. D1 against D0
    fig, ax = plt.subplots(figsize=(6.5, 5.5))
    sc = ax.scatter(df["D0"], df["D1"], c=df["evenness"], cmap="viridis", s=10, alpha=0.75, vmin=0, vmax=1)
    lim = [0.8, df["D0"].max() * 1.5]
    ax.plot(lim, lim, color="crimson", lw=1, ls="--", label="D1 = D0 (perfectly even)")
    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("D0  (datasets with nonzero storage)")
    ax.set_ylabel("D1  (effective number of datasets by storage bytes)")
    ax.set_title(
        f"Byte-weighted vs. plain dataset count  ({len(df):,} files, {report['dandisets']} dandisets)", fontsize=9
    )
    fig.colorbar(sc, ax=ax, label="evenness  D1 / D0")
    ax.legend(loc="upper left", fontsize=8)
    fig.tight_layout()
    fig.savefig(out / "1_D1_vs_D0.png", dpi=150)
    plt.close(fig)
    report["median_evenness"] = float(df["evenness"].median())
    report["median_D1"] = float(df["D1"].median())
    report["median_D0"] = float(df["D0"].median())

    # 2. D1 and evenness against bytes, with partial Spearmans
    def corr_block(frame: pd.DataFrame) -> dict:
        return {
            "rho_D1_bytes": float(stats.spearmanr(frame["log_D1"], frame["log_bytes"]).statistic),
            "rho_D1_D0": float(stats.spearmanr(frame["log_D1"], frame["log_D0"]).statistic),
            "rho_E_bytes": float(stats.spearmanr(frame["evenness"], frame["log_bytes"]).statistic),
            "rho_E_D0": float(stats.spearmanr(frame["evenness"], frame["log_D0"]).statistic),
            "partial_D1_bytes_given_D0": partial_spearman(frame["log_D1"], frame["log_bytes"], frame["log_D0"]),
            "partial_D1_D0_given_bytes": partial_spearman(frame["log_D1"], frame["log_D0"], frame["log_bytes"]),
            "partial_E_bytes_given_D0": partial_spearman(frame["evenness"], frame["log_bytes"], frame["log_D0"]),
            "rho_D0_bytes": float(stats.spearmanr(frame["log_D0"], frame["log_bytes"]).statistic),
        }

    report["corr_files_capped"] = corr_block(df)
    report["corr_dandiset_medians"] = corr_block(by_dandiset)
    c, cd = report["corr_files_capped"], report["corr_dandiset_medians"]
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    axes[0].scatter(df["total_storage_bytes"], df["D1"], s=8, alpha=0.6)
    axes[0].set_xscale("log")
    axes[0].set_yscale("log")
    axes[0].set_xlabel("total storage bytes")
    axes[0].set_ylabel("D1")
    axes[0].set_title("D1 against bytes", fontsize=10)
    axes[0].text(
        0.02,
        0.98,
        f"files (≤{args.max_per_dandiset}/dandiset), n={len(df):,}\n"
        f"  Spearman(D1, bytes) = {c['rho_D1_bytes']:+.2f}\n"
        f"  partial(D1, bytes | D0) = {c['partial_D1_bytes_given_D0']:+.2f}\n"
        f"  partial(D1, D0 | bytes) = {c['partial_D1_D0_given_bytes']:+.2f}\n"
        f"dandiset medians, n={len(by_dandiset)}\n"
        f"  Spearman(D1, bytes) = {cd['rho_D1_bytes']:+.2f}\n"
        f"  partial(D1, bytes | D0) = {cd['partial_D1_bytes_given_D0']:+.2f}\n"
        f"  partial(D1, D0 | bytes) = {cd['partial_D1_D0_given_bytes']:+.2f}",
        transform=axes[0].transAxes,
        va="top",
        fontsize=7.5,
        family="monospace",
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "0.8"},
    )
    axes[1].scatter(df["total_storage_bytes"], df["evenness"], s=8, alpha=0.6, color="tab:orange")
    axes[1].set_xscale("log")
    axes[1].set_xlabel("total storage bytes")
    axes[1].set_ylabel("evenness  D1 / D0")
    axes[1].set_title("Evenness against bytes", fontsize=10)
    axes[1].text(
        0.02,
        0.98,
        f"files: Spearman(E, bytes) = {c['rho_E_bytes']:+.2f}, partial(E, bytes | D0) = {c['partial_E_bytes_given_D0']:+.2f}\n"
        f"dandiset medians: Spearman(E, bytes) = {cd['rho_E_bytes']:+.2f}, partial = {cd['partial_E_bytes_given_D0']:+.2f}",
        transform=axes[1].transAxes,
        va="top",
        fontsize=7.5,
        family="monospace",
        bbox={"facecolor": "white", "alpha": 0.85, "edgecolor": "0.8"},
    )
    fig.tight_layout()
    fig.savefig(out / "2_D1_and_evenness_vs_bytes.png", dpi=150)
    plt.close(fig)

    # 3. ECDF of evenness
    fig, ax = plt.subplots(figsize=(6.5, 4.5))
    for values, label in (
        (df["evenness"], f"files (n={len(df):,})"),
        (by_dandiset["evenness"], f"dandiset medians (n={len(by_dandiset)})"),
    ):
        ordered = np.sort(values.dropna().to_numpy())
        ax.step(ordered, np.arange(1, len(ordered) + 1) / len(ordered), where="post", label=label)
    ax.set_xlabel("evenness  D1 / D0")
    ax.set_ylabel("cumulative fraction")
    ax.set_xlim(0, 1)
    ax.legend()
    ax.set_title("Most files concentrate their bytes in a few datasets", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "3_evenness_ecdf.png", dpi=150)
    plt.close(fig)

    # 4. Lorenz curves for eight files spanning the evenness quantiles
    datasets = pd.read_parquet(args.cache_dir / "datasets.parquet", columns=["content_id", "storage_bytes"])
    ordered_files = df.sort_values("evenness").reset_index(drop=True)
    picks = ordered_files.iloc[np.linspace(0, len(ordered_files) - 1, 8).round().astype(int)]
    fig, axes = plt.subplots(2, 4, figsize=(13, 6.5), sharex=True, sharey=True)
    for ax, (_, row) in zip(axes.ravel(), picks.iterrows()):
        storage = np.sort(datasets.loc[datasets["content_id"] == row["content_id"], "storage_bytes"].to_numpy())
        storage = storage[storage > 0]
        cumulative = np.concatenate([[0], np.cumsum(storage) / storage.sum()])
        x = np.linspace(0, 1, len(cumulative))
        ax.plot(x, cumulative, color="tab:blue")
        ax.plot([0, 1], [0, 1], color="0.6", ls="--", lw=0.8)
        ax.set_title(
            f"DANDI:{int(row['dandiset_id']):06d}\nD0={int(row['D0'])}  D1={row['D1']:.1f}  E={row['evenness']:.2f}",
            fontsize=8,
        )
    for ax in axes[1]:
        ax.set_xlabel("fraction of datasets (smallest first)")
    for ax in axes[:, 0]:
        ax.set_ylabel("fraction of storage bytes")
    fig.suptitle("Lorenz curves of dataset byte shares, files at evenly spaced evenness quantiles", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "4_lorenz_small_multiples.png", dpi=150)
    plt.close(fig)

    # 5. Section composition: dandiset stacked bars and a CLR PCA
    share_cols = [f"share_{s}" for s in SECTIONS]
    medians = df.groupby("dandiset_id")[share_cols].median()
    medians = medians.div(medians.sum(axis=1).replace(0, np.nan), axis=0).sort_values("share_acquisition")
    colors = plt.get_cmap("tab10").colors
    fig, ax = plt.subplots(figsize=(8, max(6, len(medians) * 0.06)))
    left = np.zeros(len(medians))
    for i, col in enumerate(share_cols):
        ax.barh(
            np.arange(len(medians)),
            medians[col],
            left=left,
            height=1.0,
            color=colors[i % 10],
            label=col.removeprefix("share_"),
        )
        left += medians[col].fillna(0).to_numpy()
    ax.set_yticks([])
    ax.set_ylim(-0.5, len(medians) - 0.5)
    ax.set_ylabel(f"dandisets (n={len(medians)}), sorted by acquisition share")
    ax.set_xlabel("median storage-byte share, renormalized")
    ax.set_xlim(0, 1)
    ax.legend(fontsize=7, loc="lower right", ncol=2)
    ax.set_title("Where the bytes live, per dandiset", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "5a_section_composition.png", dpi=150)
    plt.close(fig)

    present = [col for col in share_cols if (df[col] > 0).any()]
    shares = df[present].fillna(0).to_numpy() + 1e-4
    shares = shares / shares.sum(axis=1, keepdims=True)
    clr = np.log(shares) - np.log(shares).mean(axis=1, keepdims=True)
    centered = clr - clr.mean(axis=0)
    _, singular, vt = np.linalg.svd(centered, full_matrices=False)
    scores = centered @ vt[:2].T
    explained = singular**2 / np.sum(singular**2)
    codes = pd.Categorical(df["dandiset_id"]).codes
    fig, ax = plt.subplots(figsize=(7, 6))
    ax.scatter(scores[:, 0], scores[:, 1], c=codes, cmap="nipy_spectral", s=8, alpha=0.7)
    # Scale the loadings so every arrow tip lands inside the scatter, toward whichever side it points.
    # A single scale from the largest loading let the two longest arrows run off the axes, where an
    # annotation is clipped without moving the limits.
    room = [
        (scores[:, k].max() if vt[k, j] > 0 else -scores[:, k].min()) / abs(vt[k, j])
        for j in range(len(present))
        for k in range(2)
        if vt[k, j] != 0
    ]
    scale = 0.85 * min(room)
    for j, col in enumerate(present):
        ax.annotate(
            "",
            xy=(vt[0, j] * scale, vt[1, j] * scale),
            xytext=(0, 0),
            arrowprops={"arrowstyle": "->", "color": "black"},
        )
        ax.text(vt[0, j] * scale * 1.08, vt[1, j] * scale * 1.08, col.removeprefix("share_"), fontsize=8)
    ax.set_xlabel(f"PC1 ({100 * explained[0]:.0f}%)")
    ax.set_ylabel(f"PC2 ({100 * explained[1]:.0f}%)")
    ax.set_title("CLR-PCA of section byte shares (one point per file, colored by dandiset)", fontsize=9)
    fig.tight_layout()
    fig.savefig(out / "5b_section_clr_pca.png", dpi=150)
    plt.close(fig)
    report["clr_pca_explained"] = [float(v) for v in explained[:3]]

    # 6. Validation panel: between-dandiset variance fraction and agreement with count and bytes
    metrics = {
        "log D0": "log_D0",
        "log bytes": "log_bytes",
        "log cophenetic": "log_cophenetic",
        "log D1": "log_D1",
        "log D2": "log_D2",
        "r1 = D1/D0 (evenness)": "r1",
        "r2 = D2/D1": "r2",
        "r* = D∞/D2": "r_star",
    }
    rows = []
    for label, col in metrics.items():
        rows.append(
            {
                "metric": label,
                "ICC_dandiset": icc1(df[col].to_numpy(), df["dandiset_id"].to_numpy()),
                "rho_logD0_files": float(stats.spearmanr(df[col], df["log_D0"]).statistic),
                "rho_logbytes_files": float(stats.spearmanr(df[col], df["log_bytes"]).statistic),
                "rho_logD0_dandisets": float(stats.spearmanr(by_dandiset[col], by_dandiset["log_D0"]).statistic),
                "rho_logbytes_dandisets": float(stats.spearmanr(by_dandiset[col], by_dandiset["log_bytes"]).statistic),
            }
        )
    validation = pd.DataFrame(rows)
    report["validation"] = validation.round(3).to_dict(orient="records")
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.6), sharey=True)
    y = np.arange(len(validation))[::-1]
    axes[0].scatter(validation["ICC_dandiset"], y, color="black")
    axes[0].set_xlim(0, 1)
    axes[0].set_title("between-dandiset variance fraction (ICC1)", fontsize=9)
    for ax, key, title in (
        (axes[1], "logD0", "Spearman with log D0"),
        (axes[2], "logbytes", "Spearman with log bytes"),
    ):
        ax.scatter(validation[f"rho_{key}_files"], y, color="tab:blue", label="files (capped)")
        ax.scatter(validation[f"rho_{key}_dandisets"], y, color="tab:orange", marker="s", label="dandiset medians")
        ax.axvline(0, color="0.7", lw=0.8)
        ax.set_xlim(-1, 1)
        ax.set_title(title, fontsize=9)
    axes[2].legend(fontsize=7, loc="upper left")
    axes[0].set_yticks(y)
    axes[0].set_yticklabels(validation["metric"])
    for ax in axes:
        ax.grid(axis="x", color="0.9")
    fig.suptitle("Is the metric a dandiset property, and is it count or size again?", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "6_validation_panel.png", dpi=150)
    plt.close(fig)

    # 7. Optional: dandiset-level access, medians only
    dl = by_dandiset.copy()
    dl = dl[dl["number_of_requests"] > 0]
    dl["download_fraction"] = dl["number_of_downloads"] / dl["number_of_requests"]
    dl["streaming"] = (dl["number_of_requests"] - dl["number_of_downloads"]).clip(lower=0)
    access_corr = {}
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    for i, (xcol, xlabel) in enumerate((("D1", "median D1"), ("evenness", "median evenness"))):
        for j, (ycol, ylabel) in enumerate(
            (("download_fraction", "median download fraction"), ("streaming", "median streaming requests"))
        ):
            ax = axes[i, j]
            ax.scatter(dl[xcol], dl[ycol], s=12, alpha=0.7)
            if xcol == "D1":
                ax.set_xscale("log")
            if ycol == "streaming":
                ax.set_yscale("symlog")
                ax.set_ylim(bottom=0)  # a count; symlog otherwise pads into negatives
            rho = float(stats.spearmanr(dl[xcol], dl[ycol]).statistic)
            access_corr[f"{xcol}~{ycol}"] = rho
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel)
            ax.set_title(f"Spearman = {rho:+.2f}  (n={len(dl)} dandisets)", fontsize=9)
    fig.suptitle("Dandiset-level access against byte-weighted structure (medians; exploratory)", fontsize=10)
    fig.tight_layout()
    fig.savefig(out / "7_access_dandiset_level.png", dpi=150)
    plt.close(fig)
    dl["download_fraction_D0"] = dl["download_fraction"]
    access_corr["log_bytes~download_fraction"] = float(
        stats.spearmanr(dl["log_bytes"], dl["download_fraction"]).statistic
    )
    access_corr["log_D0~download_fraction"] = float(stats.spearmanr(dl["log_D0"], dl["download_fraction"]).statistic)
    access_corr["partial_D1~download_fraction|D0,bytes"] = float(
        _partial_two(dl["log_D1"], dl["download_fraction"], dl[["log_D0", "log_bytes"]].to_numpy())
    )
    report["access_dandiset_level"] = access_corr

    (out / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def _partial_two(x: pd.Series, y: pd.Series, z: np.ndarray) -> float:
    """Spearman of x and y controlling for several covariates, on ranks."""
    from scipy import stats

    rx, ry = stats.rankdata(x), stats.rankdata(y)
    rz = np.column_stack([stats.rankdata(col) for col in z.T])
    design = np.column_stack([np.ones(len(rx)), rz])
    res_x = rx - design @ np.linalg.lstsq(design, rx, rcond=None)[0]
    res_y = ry - design @ np.linalg.lstsq(design, ry, rcond=None)[0]
    return float(np.corrcoef(res_x, res_y)[0, 1])


# --------------------------------------------------------------------------------------------- cluster

RATIO_COLUMNS = ["r1", "r2", "r_star"]
RATIO_LABELS = {
    "r1": "r1 = D1/D0  (small-dataset dust)",
    "r2": "r2 = D2/D1  (long tail)",
    "r_star": "r* = D∞/D2  (unequal leaders)",
}
RATIO_COLORS = {"r1": "tab:blue", "r2": "tab:orange", "r_star": "tab:green"}


def _modality(asset_path: str) -> str:
    """The BIDS-style modality suffix of an NWB asset name (``behavior``, ``ecephys+image``), or '' when absent."""
    stem = pathlib.PurePosixPath(str(asset_path)).name.removesuffix(".nwb")
    token = stem.rsplit("_", 1)[-1] if "_" in stem else ""
    modality = token if token and "-" not in token else ""
    return modality


MODALITY_FAMILIES = ("ecephys", "icephys", "ophys", "multimodal", "image", "behavior", "other", "unlabeled")
_FAMILY_TOKENS = {
    "ecephys": {"ecephys", "spikesorting"},
    "icephys": {"icephys"},
    "ophys": {"ophys", "photometry", "fib", "widefield"},
}


def _modality_family(modality: str) -> str:
    """
    The recording family a modality suffix belongs to.

    A file belongs to the physiology it records: ``ecephys`` (with ``spikesorting``, derived from it), ``icephys``,
    or ``ophys`` (with fiber ``photometry``/``fib`` and ``widefield`` imaging). Tokens riding along, such as
    ``behavior`` or ``ogen``, do not move it. A file recording more than one is ``multimodal``. Without any of
    them it is ``image``, then ``behavior``, then ``other``, and a file with no suffix at all is ``unlabeled``.
    """
    if not modality:
        return "unlabeled"
    tokens = set(modality.split("+"))
    recorded = [family for family, members in _FAMILY_TOKENS.items() if tokens & members]
    if len(recorded) > 1:
        family = "multimodal"
    elif recorded:
        family = recorded[0]
    elif "image" in tokens:
        family = "image"
    elif "behavior" in tokens:
        family = "behavior"
    else:
        family = "other"
    return family


def _silhouette(distances: np.ndarray, labels: np.ndarray) -> float:
    """Mean silhouette width; a point alone in its cluster scores 0, as is conventional."""
    scores = []
    for i, label in enumerate(labels):
        own = labels == label
        if own.sum() < 2:
            scores.append(0.0)
            continue
        a = distances[i, own].sum() / (own.sum() - 1)
        b = min(distances[i, labels == other].mean() for other in np.unique(labels) if other != label)
        denominator = max(a, b)
        scores.append(0.0 if denominator == 0 else (b - a) / denominator)
    mean_score = float(np.mean(scores))
    return mean_score


SPLIT_BASES = ("modality", "schema", "size", "shape")


def file_schemas(datasets: pd.DataFrame) -> dict[str, frozenset]:
    """
    Each file's schema: its set of dataset paths with every run of digits replaced by ``#``.

    Numbering is what varies between files of one type, such as ``/acquisition/sweep_0012`` in a longer
    intracellular recording or ``PlaneSegmentation1`` in a second imaging plane, so it is folded away and two
    files of one type share a schema however long they ran.
    """
    normalized = datasets["path"].str.replace(r"\d+", "#", regex=True)
    schemas = normalized.groupby(datasets["content_id"]).agg(frozenset).to_dict()
    return schemas


def _majority_schema(content_ids: list[str], schemas: dict[str, frozenset]) -> set[str]:
    """The paths present in at least half of a cluster's files."""
    sets = [schemas[c] for c in content_ids if c in schemas]
    counts = pd.Series([path for paths in sets for path in paths]).value_counts()
    majority = set(counts[counts >= len(sets) / 2].index)
    return majority


def _split_basis(
    clusters: list[pd.DataFrame], schemas: dict[str, frozenset], max_schema_jaccard: float, min_size_ratio: float
) -> tuple[str, float]:
    """
    Name what distinguishes a candidate split's clusters, from most to least like a difference in file type.

    ``modality``: each cluster's most common modality suffix is named and they all differ, so the uploader
    labeled them as different file types. ``schema``: even the most alike pair of clusters shares a Jaccard
    similarity below ``max_schema_jaccard`` between their majority schemas, so the files hold different
    datasets. ``size``: the schemas agree but median storage differs by at least ``min_size_ratio``, which r1
    tracks, so one file type recorded at different lengths separates on it. ``shape``: only the arrangement
    of bytes differs. Also returns that most-alike Jaccard similarity.
    """
    majority = [_majority_schema(members["content_id"].tolist(), schemas) for members in clusters]
    jaccard = min(len(a & b) / len(a | b) if a | b else 1.0 for i, a in enumerate(majority) for b in majority[i + 1 :])
    leads = []
    for members in clusters:
        named = members.loc[members["modality"] != "", "modality"]
        leads.append(named.mode().iloc[0] if len(named) else "")
    if all(leads) and len(set(leads)) == len(leads):
        return "modality", jaccard
    if jaccard < max_schema_jaccard:
        return "schema", jaccard
    size = [members["total_storage_bytes"].median() for members in clusters]
    if max(size) >= min_size_ratio * min(size):
        return "size", jaccard
    return "shape", jaccard


def cluster_within_dandisets(
    per_file: pd.DataFrame,
    min_size: int,
    max_clusters: int,
    min_silhouette: float,
    min_separation: float,
    split_on: set[str],
    schemas: dict[str, frozenset],
    max_schema_jaccard: float,
    min_size_ratio: float,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a dandiset into groups of files only where it clearly holds different kinds of file.

    Files are placed by the logs of r1, r2 and r*, because the ratios multiply (log D_inf = log D0 + log r1 +
    log r2 + log r*), and each log is scaled by its spread across every file, so that a split demands the same
    separation in every dandiset. Ward clustering is tried for 2 up to ``max_clusters`` clusters. A candidate
    split needs every cluster to hold at least ``min_size`` files, a mean silhouette of at least
    ``min_silhouette``, and its closest pair of centroids at least ``min_separation`` global spreads apart.

    Clear clusters in ratio space are not by themselves different file types, because r1 falls with file size,
    so one file type recorded at different lengths separates cleanly. A candidate is therefore classified by
    ``_split_basis`` and kept only when its basis is in ``split_on``; otherwise the dandiset stays one group.
    The per-file table records every candidate cluster and its basis, kept or not, alongside the final group.
    """
    from scipy.cluster import hierarchy
    from scipy.spatial.distance import pdist, squareform

    frame = per_file.dropna(subset=RATIO_COLUMNS).reset_index(drop=True).copy()
    frame["modality"] = frame["asset_path"].map(_modality)
    frame["family"] = frame["modality"].map(_modality_family)
    logs = np.log10(frame[RATIO_COLUMNS].clip(lower=1e-12).to_numpy())
    spread = logs.std(axis=0)
    spread[spread == 0] = 1.0
    features = logs / spread

    frame["ratio_cluster"] = pd.array([pd.NA] * len(frame), dtype="Int64")
    frame["split_basis"] = ""
    frame["cluster"] = pd.array([pd.NA] * len(frame), dtype="Int64")
    frame["group"] = ""
    split_stats: dict[int, tuple[float, float, str, float]] = {}
    for dandiset_id, positions in frame.groupby("dandiset_id").indices.items():
        x = features[positions]
        best = None
        if len(positions) >= 2 * min_size and pdist(x).max() > 1e-9:
            linkage = hierarchy.linkage(x, method="ward")
            distances = squareform(pdist(x))
            for k in range(2, min(max_clusters, len(positions) // min_size) + 1):
                labels = hierarchy.fcluster(linkage, k, criterion="maxclust")
                sizes = np.bincount(labels)[1:]
                if len(sizes) != k or sizes.min() < min_size:
                    continue
                centroids = np.array([x[labels == c].mean(axis=0) for c in range(1, k + 1)])
                separation = float(pdist(centroids).min())
                silhouette = _silhouette(distances, labels)
                if silhouette >= min_silhouette and separation >= min_separation:
                    if best is None or silhouette > best[0]:
                        best = (silhouette, separation, labels)
        name = f"{int(dandiset_id):06d}"
        frame.loc[positions, "group"] = name
        if best is None:
            continue
        silhouette, separation, labels = best
        # Number clusters from the largest down, ties broken by r1, so names are stable from run to run
        order = sorted(
            np.unique(labels),
            key=lambda c: (-(labels == c).sum(), frame.loc[positions[labels == c], "r1"].mean()),
        )
        members_by_cluster = [positions[labels == old_label] for old_label in order]
        basis, jaccard = _split_basis(
            [frame.loc[m] for m in members_by_cluster], schemas, max_schema_jaccard, min_size_ratio
        )
        frame.loc[positions, "split_basis"] = basis
        for new_index, members in enumerate(members_by_cluster):
            frame.loc[members, "ratio_cluster"] = new_index
        if basis not in split_on:
            continue
        split_stats[int(dandiset_id)] = (silhouette, separation, basis, jaccard)
        for new_index, members in enumerate(members_by_cluster):
            frame.loc[members, "cluster"] = new_index
            frame.loc[members, "group"] = f"{name}-{new_index}"

    rows = []
    for group_name, members in frame.groupby("group", sort=True):
        dandiset_id = int(members["dandiset_id"].iloc[0])
        counts = members["modality"].replace("", "(none)").value_counts()
        silhouette, separation, basis, jaccard = split_stats.get(dandiset_id, (np.nan, np.nan, "", np.nan))
        rows.append(
            {
                "group": group_name,
                "dandiset_id": dandiset_id,
                "cluster": members["cluster"].iloc[0],
                "n_files": len(members),
                "r1": members["r1"].mean(),
                "r2": members["r2"].mean(),
                "r_star": members["r_star"].mean(),
                "median_D0": members["D0"].median(),
                "median_storage_bytes": members["total_storage_bytes"].median(),
                "family": min(
                    members["family"].mode(), key=MODALITY_FAMILIES.index
                ),  # most common family, ties going to the earlier one in MODALITY_FAMILIES
                "modalities": "; ".join(f"{modality}×{count}" for modality, count in counts.items()),
                "split_basis": basis,
                "silhouette": silhouette,
                "separation": separation,
                "schema_jaccard": jaccard,
            }
        )
    by_group = pd.DataFrame(rows)
    keep = ["content_id", "dandiset_id", "asset_path", "modality", "family", "group", "cluster", "ratio_cluster"]
    keep += ["split_basis"]
    keep += ["D0", "D1", "D2", "D_inf", *RATIO_COLUMNS, "total_storage_bytes"]
    by_file = frame[keep].sort_values(["dandiset_id", "group", "content_id"]).reset_index(drop=True)
    return by_file, by_group


def _ratio_steps(table: pd.DataFrame) -> pd.DataFrame:
    """The decades each ratio removes on the way from D0 down to D_inf: -log10 r, which sum to log10(D0/D_inf)."""
    steps = pd.DataFrame({col: -np.log10(table[col].clip(lower=1e-12, upper=1.0)) for col in RATIO_COLUMNS})
    return steps


def _row_tree(by_group: pd.DataFrame) -> tuple[list[int], list[tuple[list[float], list[float], str]], dict[int, float]]:
    """
    Order the group rows the way a clustered heatmap orders genes, and return the tree to draw beside them.

    Dandisets are leaves of a Ward tree built on their mean log ratios, each log scaled by its spread, as the
    within-dandiset clustering does. A split dandiset's clusters stay adjacent and are first joined by a short
    bracket of their own, a clade, which then attaches to the tree like any other leaf. Every merge of the tree
    is lifted by that bracket's depth so no line runs backwards. Returns the row order (by_group index labels),
    the line segments as (heights, rows, kind) with kind "clade" or "tree", and each split dandiset's mid row.
    """
    from scipy.cluster import hierarchy

    logs = np.log10(by_group[RATIO_COLUMNS].clip(lower=1e-12, upper=1.0))
    features = (logs / logs.std(axis=0).replace(0, 1)).to_numpy()
    dandisets = sorted(by_group["dandiset_id"].unique())
    member_rows = {d: by_group.index[by_group["dandiset_id"] == d] for d in dandisets}
    profiles = np.array(
        [
            np.average(
                features[by_group.index.get_indexer(member_rows[d])],
                axis=0,
                weights=by_group.loc[member_rows[d], "n_files"],
            )
            for d in dandisets
        ]
    )
    linkage = hierarchy.linkage(profiles, method="ward")
    order: list[int] = []
    for leaf in hierarchy.leaves_list(linkage):
        rows = by_group.loc[member_rows[dandisets[leaf]]].sort_values("cluster", na_position="first")
        order.extend(rows.index.tolist())
    row_of = {label: position for position, label in enumerate(order)}

    clade_depth = 0.12 * float(linkage[:, 2].max() or 1.0)
    segments: list[tuple[list[float], list[float], str]] = []
    node_row: dict[int, float] = {}
    node_height: dict[int, float] = {}
    split_mid: dict[int, float] = {}
    for leaf, d in enumerate(dandisets):
        rows = [row_of[label] for label in member_rows[d]]
        if len(rows) == 1:
            node_row[leaf], node_height[leaf] = float(rows[0]), 0.0
            continue
        for row in rows:
            segments.append(([0.0, clade_depth], [row, row], "clade"))
        segments.append(([clade_depth, clade_depth], [min(rows), max(rows)], "clade"))
        node_row[leaf] = split_mid[d] = (min(rows) + max(rows)) / 2
        node_height[leaf] = clade_depth
    n = len(dandisets)
    for i, (a, b, distance, _) in enumerate(linkage):
        a, b, height = int(a), int(b), clade_depth + float(distance)
        for child in (a, b):
            segments.append(([node_height[child], height], [node_row[child], node_row[child]], "tree"))
        segments.append(([height, height], [node_row[a], node_row[b]], "tree"))
        node_row[n + i] = (node_row[a] + node_row[b]) / 2
        node_height[n + i] = height
    return order, segments, split_mid


def _composition_bars(bar_axes: list, groups: pd.DataFrame, rows: np.ndarray) -> None:
    """Draw each group's drop from D0 to D_inf in decades (first axis) and as shares (second axis), at ``rows``."""
    steps = _ratio_steps(groups)
    total = steps.sum(axis=1)
    shares = steps.div(total.where(total > 0), axis=0).fillna(0)
    for ax, values, xlabel, title in (
        (bar_axes[0], steps, "decades from D0 down to D∞   (log10 D0/D∞)", "How far each group collapses"),
        (bar_axes[1], shares, "share of the drop from D0 to D∞", "Which ratio does the collapsing"),
    ):
        left = np.zeros(len(groups))
        for col in RATIO_COLUMNS:
            widths = values[col].to_numpy()
            ax.barh(rows, widths, left=left, height=1.0, color=RATIO_COLORS[col], label=RATIO_LABELS[col])
            left += widths
        ax.set_xlabel(xlabel)
        ax.set_title(title, fontsize=10)
        ax.tick_params(axis="y", left=False, labelleft=False)
    bar_axes[1].set_xlim(0, 1)


def cluster(args: argparse.Namespace) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    per_file = pd.read_csv(args.metrics)
    per_file = per_file[per_file["D0"] > 0]
    by_file, by_group = cluster_within_dandisets(
        per_file,
        min_size=args.min_cluster_size,
        max_clusters=args.max_clusters,
        min_silhouette=args.min_silhouette,
        min_separation=args.min_separation,
        split_on=set(args.split_on),
        schemas=file_schemas(pd.read_parquet(args.cache_dir / "datasets.parquet", columns=["content_id", "path"])),
        max_schema_jaccard=args.max_schema_jaccard,
        min_size_ratio=args.min_size_ratio,
    )
    by_file.to_csv(HERE / "byte_weighted_ratios_by_file.csv", index=False)
    by_group.to_csv(HERE / "byte_weighted_ratios_by_group.csv", index=False)

    split = by_group[by_group["cluster"].notna()]
    split_dandisets = split["dandiset_id"].unique()
    candidates = by_file[by_file["split_basis"] != ""].groupby("dandiset_id")["split_basis"].first()
    summary = {
        "files": len(by_file),
        "dandisets": int(by_file["dandiset_id"].nunique()),
        "groups": len(by_group),
        "candidate_splits_by_basis": {basis: int((candidates == basis).sum()) for basis in SPLIT_BASES},
        "split_on": sorted(args.split_on),
        "dandisets_split": len(split_dandisets),
        "clusters_in_split_dandisets": int(len(split)),
        "split_sizes": {int(k): int(v) for k, v in split.groupby("dandiset_id").size().value_counts().items()},
        "criteria": {
            "min_cluster_size": args.min_cluster_size,
            "max_clusters": args.max_clusters,
            "min_silhouette": args.min_silhouette,
            "min_separation_in_global_spreads": args.min_separation,
            "max_schema_jaccard": args.max_schema_jaccard,
            "min_size_ratio": args.min_size_ratio,
        },
    }
    out = args.out_dir
    out.mkdir(parents=True, exist_ok=True)
    (out / "ratios_summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))

    # 8. Composition of each group's drop from D0 to D_inf, in blocks by recording family, each block ordered by r1
    families = [family for family in MODALITY_FAMILIES if (by_group["family"] == family).any()]
    gap = 2.0
    order, positions, blocks, cursor = [], [], [], 0.0
    for family in families:
        members = by_group[by_group["family"] == family].sort_values("r1", ascending=False)
        first = cursor
        for label in members.index:
            order.append(label)
            positions.append(cursor)
            cursor += 1.0
        blocks.append((family, len(members), first, cursor - 1.0))
        cursor += gap
    positions = np.array(positions)
    fig = plt.figure(figsize=(15, max(8, cursor * 0.03)))
    grid = fig.add_gridspec(1, 3, width_ratios=[0.9, 3, 3], wspace=0.03, top=0.955, bottom=0.04)
    label_ax = fig.add_subplot(grid[0])
    bar_axes = [fig.add_subplot(grid[1], sharey=label_ax), fig.add_subplot(grid[2], sharey=label_ax)]
    for family, count, first, last in blocks:
        label_ax.plot([0.82, 0.82], [first - 0.3, last + 0.3], color="0.25", lw=1.2)
        for end_row in (first - 0.3, last + 0.3):
            label_ax.plot([0.82, 0.9], [end_row, end_row], color="0.25", lw=1.2)
        label_ax.text(0.76, (first + last) / 2, f"{family} ({count})", ha="right", va="center", fontsize=9)
    split_rows = positions[[pd.notna(by_group.loc[label, "cluster"]) for label in order]]
    for row in split_rows:
        label_ax.plot([0.93, 1.0], [row, row], color="crimson", lw=1.4, solid_capstyle="butt")
    label_ax.set_xlim(0, 1)
    label_ax.axis("off")
    _composition_bars(bar_axes, by_group.loc[order], positions)
    label_ax.set_ylim(cursor - gap + 0.5, -0.5)
    handles, labels = bar_axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8, bbox_to_anchor=(0.6, 0.99), frameon=False)
    fig.suptitle(
        f"Hill-ratio composition per dandiset group ({len(by_group)} groups), in blocks by recording family "
        "(groups per family in brackets), ordered by r1 within each; red ticks mark clusters of split dandisets",
        fontsize=10,
        y=1.0,
    )
    fig.savefig(out / "8_ratio_composition.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # 8b. The same composition with rows ordered by a tree like a clustered heatmap
    order, segments, split_mid = _row_tree(by_group)
    rows = np.arange(len(order), dtype=float)
    fig = plt.figure(figsize=(15, max(8, len(order) * 0.03)))
    grid = fig.add_gridspec(1, 3, width_ratios=[1.6, 3, 3], wspace=0.03, top=0.955, bottom=0.04)
    tree_ax = fig.add_subplot(grid[0])
    bar_axes = [fig.add_subplot(grid[1], sharey=tree_ax), fig.add_subplot(grid[2], sharey=tree_ax)]
    for heights, ys, kind in segments:
        if kind == "clade":
            tree_ax.plot([-h for h in heights], ys, color="crimson", lw=1.6, solid_capstyle="butt")
        else:
            tree_ax.plot([-h for h in heights], ys, color="0.35", lw=0.5)
    tree_ax.set_xlim(right=0)
    tree_ax.set_xticks([])
    for side in ("top", "right", "bottom", "left"):
        tree_ax.spines[side].set_visible(False)
    tree_ax.set_yticks([])
    tree_ax.set_ylabel(
        f"groups (n={len(by_group)}): {summary['dandisets'] - len(split_dandisets)} whole dandisets and "
        f"{len(split)} clusters from {len(split_dandisets)} split ones (red clades)",
        fontsize=8,
    )
    _composition_bars(bar_axes, by_group.loc[order], rows)
    for d, mid in split_mid.items():
        bar_axes[1].text(1.01, mid, f"{int(d):06d}", va="center", fontsize=5.5, color="crimson", clip_on=False)
    tree_ax.set_ylim(len(order) - 0.5, -0.5)
    handles, labels = bar_axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=3, fontsize=8, bbox_to_anchor=(0.6, 0.99), frameon=False)
    fig.suptitle(
        "Hill-ratio composition per dandiset group, ordered by a Ward tree on (log r1, log r2, log r*)",
        fontsize=11,
        y=1.0,
    )
    fig.savefig(out / "8b_ratio_composition_tree.png", dpi=150, bbox_inches="tight")
    plt.close(fig)

    # 9. The split dandisets alone, labeled and banded by dandiset, so each split can be checked against its files
    if len(split):
        split_rows = split.sort_values(["dandiset_id", "cluster"]).reset_index(drop=True)
        split_steps = _ratio_steps(split_rows)
        fig, ax = plt.subplots(figsize=(11, 1.2 + 0.32 * len(split_rows)))
        y = np.arange(len(split_rows))[::-1]
        for band, (_, members) in enumerate(split_rows.groupby("dandiset_id", sort=True)):
            if band % 2 == 0:
                rows_of_band = y[members.index]
                ax.axhspan(rows_of_band.min() - 0.5, rows_of_band.max() + 0.5, color="0.92", zorder=0)
        left = np.zeros(len(split_rows))
        for col in RATIO_COLUMNS:
            ax.barh(y, split_steps[col], left=left, height=0.8, color=RATIO_COLORS[col], label=RATIO_LABELS[col])
            left += split_steps[col].to_numpy()
        for yi, (_, row), end in zip(y, split_rows.iterrows(), left):
            ax.text(end + 0.03, yi, f"r1={row.r1:.3f}  r2={row.r2:.2f}  r*={row.r_star:.2f}", va="center", fontsize=6.5)
        labels = [
            f"{row.group}  ({row.split_basis})  n={row.n_files}  [{row.modalities}]" for _, row in split_rows.iterrows()
        ]
        ax.set_yticks(y)
        ax.set_yticklabels(labels, fontsize=7)
        ax.set_ylim(-0.5, len(split_rows) - 0.5)
        ax.set_xlim(0, left.max() * 1.45)
        ax.set_xlabel("decades from D0 down to D∞   (log10 D0/D∞)")
        ax.legend(fontsize=7, loc="lower right")
        ax.set_title(
            f"Dandisets split into file-type clusters ({len(split_dandisets)} dandisets, {len(split)} clusters)\n"
            "shaded bands group each dandiset's clusters; brackets give each cluster's modality suffixes",
            fontsize=9,
        )
        fig.tight_layout()
        fig.savefig(out / "9_split_dandisets.png", dpi=150)
        plt.close(fig)


def _walk_one(content_id: str, cache: pathlib.Path) -> None:
    """Child-process entry: walk one file, write its parquet, and print the error if it failed."""
    result = walk_file(content_id)
    if result["error"] is not None:
        print(result["error"])
        return
    rows = result["rows"] or [{"content_id": content_id}]
    pd.DataFrame(rows).to_parquet(cache / "files" / f"{content_id}.parquet", index=False)


def main() -> None:
    if len(sys.argv) == 4 and sys.argv[1] == "_walk_one":
        _walk_one(sys.argv[2], pathlib.Path(sys.argv[3]))
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("build", "plot"):
        p = sub.add_parser(name)
        p.add_argument(
            "--cache-dir",
            type=pathlib.Path,
            default=HERE / ".byte_weighted_cache",
            help="Per-file parquet cache (uncommitted; makes the walk resumable)",
        )
        p.add_argument("--max-per-dandiset", type=int, default=10, help="Stratification cap (default 10)")
    b = sub.choices["build"]
    b.add_argument("--data", type=pathlib.Path, default=HERE / "access_structure.csv")
    b.add_argument("--workers", type=int, default=16)
    b.add_argument("--seed", type=int, default=0)
    b.add_argument("--timeout", type=int, default=900, help="Seconds before one file's walk is abandoned")
    b.add_argument(
        "--skip-chunk-count",
        action="store_true",
        help="Leave n_chunks empty to halve the index traversal; for retrying the largest files",
    )
    b.add_argument("--retry-failed", action="store_true", help="Walk files that failed on an earlier run again")
    pl = sub.choices["plot"]
    pl.add_argument("--metrics", type=pathlib.Path, default=HERE / "byte_weighted.csv")
    pl.add_argument("--out-dir", type=pathlib.Path, default=HERE / "figures" / "byte_weighted")
    cl = sub.add_parser("cluster")
    cl.add_argument("--metrics", type=pathlib.Path, default=HERE / "byte_weighted.csv")
    cl.add_argument("--out-dir", type=pathlib.Path, default=HERE / "figures" / "byte_weighted")
    cl.add_argument("--min-cluster-size", type=int, default=2, help="Smallest cluster a split may produce")
    cl.add_argument("--max-clusters", type=int, default=4, help="Most clusters one dandiset may split into")
    cl.add_argument("--min-silhouette", type=float, default=0.5, help="Mean silhouette a split must reach")
    cl.add_argument(
        "--min-separation",
        type=float,
        default=1.0,
        help="Closest centroid pair must be this many global spreads apart in log-ratio space",
    )
    cl.add_argument(
        "--split-on",
        nargs="+",
        choices=SPLIT_BASES,
        default=["modality", "schema"],
        help="Which kinds of candidate split to keep; size and shape splits are not different file types",
    )
    cl.add_argument(
        "--max-schema-jaccard",
        type=float,
        default=0.7,
        help="Clusters whose majority schemas are less alike than this are different file types",
    )
    cl.add_argument(
        "--cache-dir",
        type=pathlib.Path,
        default=HERE / ".byte_weighted_cache",
        help="Holds datasets.parquet, read for each file's schema",
    )
    cl.add_argument("--min-size-ratio", type=float, default=3.0, help="Storage ratio that labels a size split")
    args = parser.parse_args()
    if args.command == "build":
        build(args)
    elif args.command == "plot":
        plot(args)
    else:
        cluster(args)


if __name__ == "__main__":
    main()

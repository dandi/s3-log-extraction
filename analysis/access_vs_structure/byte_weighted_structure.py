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
    """D0, D1, D2, evenness and top shares over the nonzero entries of one file's byte vector."""
    nonzero = storage[storage > 0].astype(np.float64)
    if nonzero.size == 0:
        return {"D0": 0, "D1": np.nan, "D2": np.nan, "evenness": np.nan, "top1_share": np.nan, "top5_share": np.nan}
    shares = nonzero / nonzero.sum()
    d1 = float(np.exp(-np.sum(shares * np.log(shares))))
    ordered = np.sort(shares)[::-1]
    metrics = {
        "D0": int(nonzero.size),
        "D1": d1,
        "D2": float(1.0 / np.sum(shares**2)),
        "evenness": d1 / nonzero.size,
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
        "evenness": "evenness",
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
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8), sharey=True)
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
    args = parser.parse_args()
    if args.command == "build":
        build(args)
    else:
        plot(args)


if __name__ == "__main__":
    main()

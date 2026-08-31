"""
Dry-run: measure the impact of excluding cloud/VPN/GitHub IPs from ``number_of_views``.

This does NOT change any production code. It reuses the library's *actual* view
sessionizer (``_collect_asset_views``), the *actual* IP cache (``load_ip_cache``),
and the *actual* exclusion predicate (``is_cloud_service_or_vpn_label``) to compute,
over the extraction cache:

  * total_views   — views exactly as the shipped ``number_of_views`` counts them
                    (this should match the published totals — a consistency check);
  * kept_views    — views that would remain after excluding IPs whose ip_to_region
                    label is a cloud/VPN service (GitHub, AWS/*, GCP/*, VPN);
  * excluded_views and the % drop, broken down by service label.

Because it calls the same functions the summaries call, ``kept_views`` is exactly
what ``number_of_views`` would become if the exclusion were wired into
``_collect_asset_views``. Run this BEFORE implementing the change to know the
magnitude, and AFTER to confirm the official output matches.

IMPORTANT: the exclusion uses the ip_to_region *cache* label, which is a different
(and possibly staler, IPv4-only) source than a direct api.github.com/meta check.
This measures the real production predicate, not the meta-API proxy.

Usage
-----
    python measure_view_exclusion_impact.py --cache-dir /path/to/cache [--no-encryption] \\
        [--dataset 000032]   # optional: restrict to one dataset for a fast first pass

Encryption password via S3_LOG_EXTRACTION_PASSWORD if the cache is encrypted.
"""

import argparse
import collections
import pathlib
import sys

import tqdm


def _load_library(cache_dir: pathlib.Path):
    """Import the exact production functions so the measurement matches the summaries."""
    sys.path.insert(0, str(pathlib.Path(__file__).parent.parent / "src"))
    from s3_log_extraction.ip_utils import is_cloud_service_or_vpn_label, load_ip_cache
    from s3_log_extraction.summarize._generate_summaries import _collect_asset_views

    return _collect_asset_views, load_ip_cache, is_cloud_service_or_vpn_label


def _service_of(label: str) -> str:
    """Coarse service name for the breakdown (GitHub / AWS / GCP / VPN / other)."""
    if not label:
        return "other"
    head = label.split("/", 1)[0]
    return head if head in {"GitHub", "AWS", "GCP", "VPN"} else "other"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument("--dataset", default=None, help="Optional dataset-name substring to restrict to")
    args = parser.parse_args()
    use_encryption = not args.no_encryption

    collect_asset_views, load_ip_cache, is_cloud_service_or_vpn_label = _load_library(args.cache_dir)

    print("Loading ip_to_region cache (the exact source the summaries use)...")
    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=args.cache_dir, use_encryption=use_encryption
    )
    print(f"  {len(ip_to_region):,} IPs in the region cache")

    extraction_root = args.cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {args.cache_dir}")

    asset_dirs = []
    for dataset_dir in sorted(extraction_root.iterdir()):
        if not dataset_dir.is_dir():
            continue
        if args.dataset and args.dataset not in dataset_dir.name:
            continue
        for asset_dir in dataset_dir.rglob("*"):
            if (asset_dir / "timestamps.txt").exists():
                asset_dirs.append(asset_dir)
    print(f"Found {len(asset_dirs)} asset directories")

    total_views = 0
    excluded_views = 0
    excluded_by_service: dict[str, int] = collections.defaultdict(int)
    excluded_ips: set[str] = set()
    per_dataset_total: dict[str, int] = collections.defaultdict(int)
    per_dataset_excluded: dict[str, int] = collections.defaultdict(int)

    for asset_dir in tqdm.tqdm(asset_dirs, desc="Sessionizing (production code path)"):
        dataset_id = asset_dir.relative_to(extraction_root).parts[0]
        views = collect_asset_views(asset_directory=asset_dir, use_encryption=use_encryption)
        for _view_date, ip in views:
            total_views += 1
            per_dataset_total[dataset_id] += 1
            label = ip_to_region.get(ip, "")
            if is_cloud_service_or_vpn_label(label):  # exact production predicate (takes the region label)
                excluded_views += 1
                per_dataset_excluded[dataset_id] += 1
                excluded_by_service[_service_of(label)] += 1
                excluded_ips.add(ip)

    kept_views = total_views - excluded_views
    pct = 100 * excluded_views / max(total_views, 1)

    print("\n--- View-exclusion impact (production sessionizer + production predicate) ---")
    print(f"  total_views (current number_of_views):      {total_views:,}")
    print(f"  kept_views  (after excluding cloud/VPN/GH):  {kept_views:,}")
    print(f"  excluded_views:                              {excluded_views:,} ({pct:.2f}%)")
    print(f"  distinct excluded IPs:                       {len(excluded_ips):,}")
    print("\n  excluded views by service label:")
    for service, count in sorted(excluded_by_service.items(), key=lambda kv: -kv[1]):
        print(f"    {service:>7}: {count:,}")

    print("\n--- Datasets most affected (by % of views excluded) ---")
    ranked = sorted(
        per_dataset_total,
        key=lambda d: per_dataset_excluded[d] / max(per_dataset_total[d], 1),
        reverse=True,
    )
    for dataset_id in ranked[:15]:
        tot = per_dataset_total[dataset_id]
        exc = per_dataset_excluded[dataset_id]
        print(f"    {dataset_id}: {exc:,}/{tot:,} excluded ({100 * exc / max(tot, 1):.1f}%)")


if __name__ == "__main__":
    main()

"""
Resolve the pseudonyms used in the behavioral profiles back to IP addresses. LOCAL USE ONLY.

``profile_ip_behavior.py`` never stores an IP address: it keeps a salted keyed hash and prints a
stable pseudonym derived from that hash (``ProudVireo22``), so its tables, plots and findings can be
shared freely. That is the right default, but acting on a finding — adding a specific actor to an
exclusion list — needs the address behind the name.

This script closes that loop on the operator's own machine. It walks the cache reading only the
``ips.txt`` files, derives each distinct address's hash and pseudonym with exactly the same salt and
function the profiler uses, and writes the mapping for the pseudonyms asked for.

    !!  The output contains RAW IP ADDRESSES. It is personal data. Keep it on the machine that       !!
    !!  produced it: do not commit it, attach it to an issue, paste it into a pull request, or       !!
    !!  share it. Share the pseudonym instead -- that is what it is for.                             !!

The mapping is reproducible at any time from the cache plus the salt, so there is no reason to keep
the file around; delete it once the exclusion list is written.

Usage
-----
    # the pseudonyms of interest, read off the profiler's tables
    python resolve_ip_aliases.py --cache-dir /path/to/cache --no-encryption \\
        --alias ProudVireo22 --alias GoldenFinch39

    # or every pseudonym in one pass
    python resolve_ip_aliases.py --cache-dir /path/to/cache --no-encryption --all

The salt must match the one used for the profiling run (``S3_LOG_EXTRACTION_SALT``, or
``S3_LOG_EXTRACTION_PASSWORD``); otherwise the pseudonyms will not line up and nothing will match.
"""

import argparse
import csv
import hashlib
import pathlib
import sys

import tqdm

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from profile_ip_behavior import _alias, _ip_hash_key, _load_library  # noqa: E402

_WARNING = (
    "\n"
    "  ##################################################################################\n"
    "  #  The file just written contains RAW IP ADDRESSES.                              #\n"
    "  #  Keep it on this machine: do not commit, attach, paste or otherwise share it.  #\n"
    "  #  Delete it once the exclusion list is written -- it is reproducible from the   #\n"
    "  #  cache and the salt at any time.                                               #\n"
    "  ##################################################################################\n"
)


def collect_ip_aliases(cache_dir: pathlib.Path, use_encryption: bool, max_assets: int | None = None) -> dict[str, str]:
    """Walk the cache and return ``{alias: ip}`` for every distinct address it contains."""
    _resolver_cls, read_ips, _timeout, _timestamp_format = _load_library()

    extraction_root = cache_dir / "extraction"
    if not extraction_root.exists():
        raise FileNotFoundError(f"No 'extraction' subdirectory under {cache_dir}")

    asset_dirs = [
        asset_dir
        for dataset_dir in sorted(extraction_root.iterdir())
        if dataset_dir.is_dir()
        for asset_dir in dataset_dir.rglob("*")
        if (asset_dir / "ips.txt").exists()
    ]
    if max_assets is not None:
        asset_dirs = asset_dirs[:max_assets]
    print(f"Found {len(asset_dirs):,} asset directories")

    seen: set[str] = set()
    skipped = 0
    for asset_dir in tqdm.tqdm(asset_dirs, desc="Reading ips.txt"):
        try:
            seen.update(read_ips(file_path=asset_dir / "ips.txt", use_encryption=use_encryption))
        except FileNotFoundError, ValueError:
            skipped += 1
    if skipped:
        print(f"  Skipped {skipped} unreadable ips.txt file(s)")

    key = _ip_hash_key()
    mapping = {}
    for ip in seen:
        ip_hash = hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest()
        mapping[_alias(ip_hash)] = ip
    print(f"Resolved {len(mapping):,} distinct addresses")
    return mapping


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache-dir", required=True, type=pathlib.Path)
    parser.add_argument("--no-encryption", action="store_true")
    parser.add_argument(
        "--alias",
        action="append",
        default=[],
        help="A pseudonym to resolve, as printed by the profiler. Repeatable.",
    )
    parser.add_argument("--all", action="store_true", help="Resolve every pseudonym rather than a chosen few")
    parser.add_argument("--max-assets", type=int, default=None, help="Layout-independent smoke test over the first N")
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path("ip_aliases.PRIVATE.csv"),
        help="Where to write the mapping. Treat it as personal data.",
    )
    args = parser.parse_args()

    if not args.alias and not args.all:
        parser.error("give at least one --alias, or --all")

    mapping = collect_ip_aliases(
        cache_dir=args.cache_dir,
        use_encryption=not args.no_encryption,
        max_assets=args.max_assets,
    )

    if args.all:
        wanted = sorted(mapping)
    else:
        wanted = list(dict.fromkeys(args.alias))
        missing = [alias for alias in wanted if alias not in mapping]
        if missing:
            print(f"\n  NOT FOUND: {', '.join(missing)}")
            print("  A pseudonym that does not resolve usually means the salt differs from the profiling run")
            print("  (S3_LOG_EXTRACTION_SALT / S3_LOG_EXTRACTION_PASSWORD), or the cache has changed.")
            wanted = [alias for alias in wanted if alias in mapping]

    if not wanted:
        print("\nNothing to write.")
        return

    with args.out.open(mode="w", newline="", encoding="utf-8") as file_stream:
        writer = csv.writer(file_stream)
        writer.writerow(["alias", "ip_address"])
        for alias in wanted:
            writer.writerow([alias, mapping[alias]])
    print(f"\nWrote {len(wanted):,} mapping(s) to {args.out}")
    print(_WARNING)


if __name__ == "__main__":
    main()

"""
Resolve the pseudonyms used in the behavioral profiles back to IP addresses. LOCAL USE ONLY.

``profile_ip_behavior.py`` never stores an IP address. It keeps a salted keyed hash, and labels each
one with a pseudonym drawn *uniformly at random* — the name is not derived from the address and says
nothing about it, so tables, plots and findings carry no information about who is who and can be
shared freely.

The only thing connecting a name to an address is the registry the profiler writes beside its cache
(``analysis_cache/alias_registry.PRIVATE.json``), which never leaves the machine that produced it.
This script joins that registry against the cache to answer the one question the published tables
deliberately cannot: which address is behind a given name, so it can be put on an exclusion list.

    !!  The output contains RAW IP ADDRESSES. It is personal data. Keep it on the machine that       !!
    !!  produced it: do not commit it, attach it to an issue, paste it into a pull request, or       !!
    !!  share it. Share the pseudonym instead -- that is what it is for.                             !!

Usage
-----
    python resolve_ip_aliases.py --cache-dir /path/to/cache --no-encryption \\
        --alias SomeName1234 --alias OtherName5678

    python resolve_ip_aliases.py --cache-dir /path/to/cache --no-encryption --all

The salt must match the one the profiling run used (``S3_LOG_EXTRACTION_SALT``, or
``S3_LOG_EXTRACTION_PASSWORD``), since the registry is keyed by the hash that salt produces.
"""

import argparse
import csv
import hashlib
import pathlib
import sys

import tqdm

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from profile_ip_behavior import ALIAS_REGISTRY_NAME, _ip_hash_key, _load_library  # noqa: E402

_WARNING = (
    "\n"
    "  ##################################################################################\n"
    "  #  The file just written contains RAW IP ADDRESSES.                              #\n"
    "  #  Keep it on this machine: do not commit, attach, paste or otherwise share it.  #\n"
    "  #  Delete it once the exclusion list is written.                                 #\n"
    "  ##################################################################################\n"
)


def collect_hash_to_ip(cache_dir: pathlib.Path, use_encryption: bool, max_assets: int | None = None) -> dict[str, str]:
    """Walk the cache and return ``{salted_hash: ip}`` for every distinct address it contains."""
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
    hash_to_ip = {hashlib.blake2b(ip.encode("utf-8"), key=key, digest_size=16).hexdigest(): ip for ip in seen}
    print(f"Hashed {len(hash_to_ip):,} distinct addresses")
    return hash_to_ip


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
        "--registry",
        type=pathlib.Path,
        default=None,
        help=f"The profiler's pseudonym registry. Defaults to <cache-dir>/analysis_cache/{ALIAS_REGISTRY_NAME}",
    )
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path("ip_aliases.PRIVATE.csv"),
        help="Where to write the mapping. Treat it as personal data.",
    )
    args = parser.parse_args()

    if not args.alias and not args.all:
        parser.error("give at least one --alias, or --all")

    import json

    registry_path = args.registry or (args.cache_dir / "analysis_cache" / ALIAS_REGISTRY_NAME)
    if not registry_path.exists():
        parser.error(
            f"No pseudonym registry at {registry_path}. It is written by profile_ip_behavior.py and is the only "
            "record linking a name to an address; without it the names cannot be resolved at all."
        )
    hash_to_alias: dict[str, str] = json.loads(registry_path.read_text(encoding="utf-8"))
    alias_to_hash = {alias: ip_hash for ip_hash, alias in hash_to_alias.items()}
    print(f"Registry holds {len(alias_to_hash):,} pseudonyms")

    wanted = sorted(alias_to_hash) if args.all else list(dict.fromkeys(args.alias))
    unknown = [alias for alias in wanted if alias not in alias_to_hash]
    if unknown:
        print(f"\n  NOT IN REGISTRY: {', '.join(unknown)}")
        print("  Either the name is mistyped, or it came from a run whose registry has since been replaced.")
        wanted = [alias for alias in wanted if alias in alias_to_hash]
    if not wanted:
        print("\nNothing to resolve.")
        return

    hash_to_ip = collect_hash_to_ip(
        cache_dir=args.cache_dir,
        use_encryption=not args.no_encryption,
        max_assets=args.max_assets,
    )

    rows = []
    unmatched = []
    for alias in wanted:
        ip = hash_to_ip.get(alias_to_hash[alias])
        if ip is None:
            unmatched.append(alias)
        else:
            rows.append((alias, ip))
    if unmatched:
        print(f"\n  IN REGISTRY BUT NOT IN THE CACHE: {', '.join(unmatched)}")
        print("  The address no longer appears in the cache, or the salt differs from the profiling run.")
    if not rows:
        print("\nNothing to write.")
        return

    with args.out.open(mode="w", newline="", encoding="utf-8") as file_stream:
        writer = csv.writer(file_stream)
        writer.writerow(["alias", "ip_address"])
        writer.writerows(rows)
    args.out.chmod(0o600)
    print(f"\nWrote {len(rows):,} mapping(s) to {args.out}")
    print(_WARNING)


if __name__ == "__main__":
    main()

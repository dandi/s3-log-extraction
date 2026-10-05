"""
Convert a hash-keyed pseudonym registry to the address-keyed format. LOCAL USE ONLY, ONCE.

Earlier runs of ``profile_ip_behavior.py`` wrote ``{salted_hash: alias}``, which left the registry
inert if it leaked but meant recovering an address took the hashing salt and a full walk of the
extraction cache. The registry is local-only either way, so that bought little and made the one
operation it exists for depend on the salt still matching. It now stores ``{ip: alias}``.

This script does that conversion while **preserving every existing pseudonym**, so the aliases in
already-published tables and figures keep pointing at the same actors and nothing has to be
regenerated. It walks the cache once, hashing each address to find the name recorded against it.
Run it once, then delete it from your working copy; ``resolve_ip_aliases.py`` needs neither the salt
nor the cache afterwards.

    !!  The registry this writes contains RAW IP ADDRESSES. It is personal data, written 0600 and    !!
    !!  gitignored. Keep it on the machine that produced it.                                        !!

Usage
-----
    S3_LOG_EXTRACTION_PASSWORD='<the one the profiling run used>' \\
        python migrate_alias_registry.py --cache-dir /path/to/cache --no-encryption

The salt must match the profiling run (``S3_LOG_EXTRACTION_SALT``, or
``S3_LOG_EXTRACTION_PASSWORD``), since the old keys are the hashes that salt produced. A mismatch
cannot corrupt anything: it simply matches nothing, and the script says so and writes no file.
"""

import argparse
import hashlib
import json
import pathlib
import shutil
import sys

import tqdm

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from profile_ip_behavior import ALIAS_REGISTRY_NAME, _ip_hash_key, _load_library  # noqa: E402

_HASH_LENGTH = 32


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
    parser.add_argument("--max-assets", type=int, default=None, help="Layout-independent smoke test over the first N")
    parser.add_argument(
        "--registry",
        type=pathlib.Path,
        default=None,
        help=f"The registry to convert. Defaults to <cache-dir>/analysis_cache/{ALIAS_REGISTRY_NAME}",
    )
    args = parser.parse_args()

    registry_path = args.registry or (args.cache_dir / "analysis_cache" / ALIAS_REGISTRY_NAME)
    if not registry_path.exists():
        parser.error(f"No pseudonym registry at {registry_path}; there is nothing to convert.")

    recorded: dict[str, str] = json.loads(registry_path.read_text(encoding="utf-8"))
    legacy_keys = [key for key in recorded if len(key) == _HASH_LENGTH]
    if not legacy_keys:
        print(f"{registry_path} is already keyed by address ({len(recorded):,} pseudonyms). Nothing to do.")
        return
    print(f"Converting {len(legacy_keys):,} hash-keyed pseudonyms from {registry_path}")

    hash_to_ip = collect_hash_to_ip(
        cache_dir=args.cache_dir,
        use_encryption=not args.no_encryption,
        max_assets=args.max_assets,
    )

    ip_to_alias = {}
    unmatched = []
    for ip_hash, alias in recorded.items():
        # A key that is not a hash is already an address, from a partially converted file
        if len(ip_hash) != _HASH_LENGTH:
            ip_to_alias[ip_hash] = alias
            continue
        ip = hash_to_ip.get(ip_hash)
        if ip is None:
            unmatched.append(alias)
        else:
            ip_to_alias[ip] = alias

    if unmatched:
        print(f"\n  {len(unmatched):,} pseudonym(s) matched no address in the cache, so their names cannot be")
        print("  carried across. Either those addresses no longer appear in the cache, or the salt differs")
        print(f"  from the profiling run. First few: {', '.join(unmatched[:5])}")
    if not ip_to_alias:
        print("\nNothing matched; the salt almost certainly differs from the profiling run. No file written.")
        return

    backup_path = registry_path.with_suffix(f"{registry_path.suffix}.hash-keyed.bak")
    shutil.copy2(src=registry_path, dst=backup_path)
    backup_path.chmod(0o600)
    registry_path.write_text(json.dumps(ip_to_alias, indent=0, sort_keys=True), encoding="utf-8")
    registry_path.chmod(0o600)
    print(f"\nCarried {len(ip_to_alias):,} pseudonyms across, unchanged, to {registry_path}")
    print(f"The hash-keyed original is kept at {backup_path}; delete it once a resolve has been verified.")


if __name__ == "__main__":
    main()

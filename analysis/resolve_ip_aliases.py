"""
Resolve the pseudonyms used in the behavioral profiles back to IP addresses. LOCAL USE ONLY.

``profile_ip_behavior.py`` publishes no IP address. Every actor appears under a pseudonym drawn
*uniformly at random* — the name is not derived from the address and says nothing about it, so
tables, plots and findings carry no information about who is who and can be shared freely.

The only thing connecting a name to an address is the registry the profiler writes beside its cache
(``analysis_cache/alias_registry.PRIVATE.json``), which never leaves the machine that produced it.
This script reads that registry to answer the one question the published tables deliberately cannot:
which address is behind a given name, so it can be put on an exclusion list.

    !!  The output contains RAW IP ADDRESSES. It is personal data. Keep it on the machine that       !!
    !!  produced it: do not commit it, attach it to an issue, paste it into a pull request, or       !!
    !!  share it. Share the pseudonym instead -- that is what it is for.                            !!

Usage
-----
    python resolve_ip_aliases.py --cache-dir /path/to/cache --alias SomeName1234 --alias OtherName5678

    python resolve_ip_aliases.py --cache-dir /path/to/cache --all

The registry holds the addresses themselves, so resolution is a lookup. It needs no hashing salt and
does not read the extraction cache. A registry written by a version before that change is keyed by
the salted hash instead; convert it once with ``migrate_alias_registry.py``.
"""

import argparse
import csv
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from profile_ip_behavior import ALIAS_REGISTRY_NAME  # noqa: E402

_WARNING = (
    "\n"
    "  ##################################################################################\n"
    "  #  The file just written contains RAW IP ADDRESSES.                              #\n"
    "  #  Keep it on this machine: do not commit, attach, paste or otherwise share it.  #\n"
    "  #  Delete it once the exclusion list is written.                                 #\n"
    "  ##################################################################################\n"
)

# A hash is 32 hex characters and an address never is, so a legacy registry is recognizable on sight
# rather than by asking the user which format they have
_HASH_LENGTH = 32


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cache-dir",
        type=pathlib.Path,
        default=None,
        help=f"Locates the registry at <cache-dir>/analysis_cache/{ALIAS_REGISTRY_NAME}. The cache itself is "
        "not read.",
    )
    parser.add_argument(
        "--registry",
        type=pathlib.Path,
        default=None,
        help="The registry to read, in place of deriving its path from --cache-dir.",
    )
    parser.add_argument(
        "--alias",
        action="append",
        default=[],
        help="A pseudonym to resolve, as printed by the profiler. Repeatable.",
    )
    parser.add_argument("--all", action="store_true", help="Resolve every pseudonym rather than a chosen few")
    parser.add_argument(
        "--out",
        type=pathlib.Path,
        default=pathlib.Path("ip_aliases.PRIVATE.csv"),
        help="Where to write the mapping. Treat it as personal data.",
    )
    args = parser.parse_args()

    if not args.alias and not args.all:
        parser.error("give at least one --alias, or --all")
    if args.registry is None and args.cache_dir is None:
        parser.error("give --cache-dir, or --registry to name the file directly")

    registry_path = args.registry or (args.cache_dir / "analysis_cache" / ALIAS_REGISTRY_NAME)
    if not registry_path.exists():
        parser.error(
            f"No pseudonym registry at {registry_path}. It is written by profile_ip_behavior.py and is the only "
            "record linking a name to an address; without it the names cannot be resolved at all."
        )
    ip_to_alias: dict[str, str] = json.loads(registry_path.read_text(encoding="utf-8"))
    if any(len(key) == _HASH_LENGTH for key in ip_to_alias):
        parser.error(
            f"{registry_path} is keyed by the salted hash, which this script no longer resolves. It cannot be "
            "read without the hashing salt and a walk of the cache. Convert it once with migrate_alias_registry.py, "
            "which preserves every existing name, and then rerun this."
        )
    alias_to_ip = {alias: ip for ip, alias in ip_to_alias.items()}
    print(f"Registry holds {len(alias_to_ip):,} pseudonyms")

    wanted = sorted(alias_to_ip) if args.all else list(dict.fromkeys(args.alias))
    unknown = [alias for alias in wanted if alias not in alias_to_ip]
    if unknown:
        print(f"\n  NOT IN REGISTRY: {', '.join(unknown)}")
        print("  Either the name is mistyped, or it came from a run whose registry has since been replaced.")
        wanted = [alias for alias in wanted if alias in alias_to_ip]
    if not wanted:
        print("\nNothing to resolve.")
        return

    rows = [(alias, alias_to_ip[alias]) for alias in wanted]
    with args.out.open(mode="w", newline="", encoding="utf-8") as file_stream:
        writer = csv.writer(file_stream)
        writer.writerow(["alias", "ip_address"])
        writer.writerows(rows)
    args.out.chmod(0o600)
    print(f"\nWrote {len(rows):,} mapping(s) to {args.out}")
    print(_WARNING)


if __name__ == "__main__":
    main()

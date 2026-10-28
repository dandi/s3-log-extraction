"""Command line interface definitions for the S3 log extraction tool."""

import inspect
import os
import pathlib
import sys
import typing

import rich_click

from ..config import (
    get_excluded_ips,
    reset_extraction,
    set_base_temporary_directory,
    set_cache_directory,
    unset_base_temporary_directory,
)
from ..extractors import (
    RemoteS3LogAccessExtractor,
    S3LogAccessExtractor,
    stop_extraction,
)
from ..ip_utils import (
    find_excluded_ip_records,
    remove_excluded_ip_records,
    update_geolite2_database,
    update_region_code_coordinates,
)
from ..summarize import (
    generate_all_dataset_totals,
    generate_archive_summaries,
    generate_archive_totals,
    generate_summaries,
)
from ..summarize.globals import REGION_DISCLOSURE_THRESHOLD
from ..testing import generate_benchmark
from ..utils import IpCategoryCount, get_extraction_completion, get_ip_stats, get_log_bucket_stats
from ..validate import (
    DownloadsLogicPreValidator,
    ExtractionHeuristicPreValidator,
    HttpEmptySplitPreValidator,
    HttpSplitCountPreValidator,
    TimestampsParsingPreValidator,
)


def _cache_directory_option(command: typing.Callable) -> typing.Callable:
    """
    Attach the shared ``--cache`` option to a command.

    This is the variant taken by commands that write into the cache directory. The three commands whose
    ``--cache`` differs, in help text or in requiring the directory to already exist, declare their own.

    Apply this where the literal option block would have gone: a decorator in a different position would
    reorder the option in the rendered help.
    """
    return rich_click.option(
        "--cache",
        "cache_directory",
        help=(
            "Use a non-default cache directory for this command. "
            "This overrides the configured cache directory without modifying saved config."
        ),
        required=False,
        type=rich_click.Path(writable=True, file_okay=False, dir_okay=True),
        default=None,
    )(command)


# The exit status of `extract --check` when log files are pending, kept apart from 1 (an unhandled error) and
# 2 (a usage error) so that a script can tell pending work apart from a failure.
_CHECK_PENDING_EXIT_CODE = 3

# Pre-validation protocols, each offered as a `s3logextraction validate <protocol> <directory>` subcommand.
_PRE_VALIDATORS: dict[str, type] = {
    "downloads_logic": DownloadsLogicPreValidator,
    "http_empty_split": HttpEmptySplitPreValidator,
    "http_split_count": HttpSplitCountPreValidator,
    "extraction_heuristic": ExtractionHeuristicPreValidator,
    "timestamps_parsing": TimestampsParsingPreValidator,
}


def _report_check(is_pending: bool, /) -> None:
    """Report the outcome of `extract --check`, exiting with the pending status when there is work to do."""
    if is_pending is False:
        rich_click.echo("No log files are pending extraction.")
        return

    rich_click.echo("Log files are pending extraction.")
    sys.exit(_CHECK_PENDING_EXIT_CODE)


# s3logextraction
@rich_click.group()
def s3logextraction_cli():
    pass


# s3logextraction extract < directory >
@s3logextraction_cli.command(name="extract")
@rich_click.argument("directory", type=rich_click.Path(writable=False))
@rich_click.option(
    "--limit",
    help="The maximum number of files to process. By default, all files will be processed.",
    required=False,
    type=rich_click.IntRange(min=1),
    default=None,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc. "
        "By default, "
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@rich_click.option(
    "--cache",
    "cache_directory",
    help=(
        "Use a non-default cache directory for this extraction run only. "
        "This overrides the configured cache directory without modifying saved config."
    ),
    required=False,
    type=rich_click.Path(writable=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--mode",
    help=(
        "Special parsing mode related to expected object key structure; "
        "By default, objects will be processed using the generic structure."
    ),
    required=False,
    type=rich_click.Choice(choices=["remote"]),
    default=None,
)
@rich_click.option(
    "--inventory",
    "inventory_directory",
    help=(
        "Path to a local pre-downloaded AWS S3 Inventory directory. "
        "The directory must contain a 'hive/' sub-folder with Hive-partitioned symlink files "
        "(e.g. hive/dt=YYYY-MM-DD-HH-MM/symlink.txt), a 'data/' sub-folder with the "
        "gzip-compressed CSV inventory files, and timestamped manifest directories "
        "(e.g. 2026-05-03T01-00Z/manifest.json). "
        "The most recent hive partition is used to discover all log files in the bucket, "
        "replacing live s5cmd ls calls."
    ),
    required=False,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Encrypt IP addresses in extraction output files. Enabled by default.",
    type=rich_click.BOOL,
    default=True,
)
@rich_click.option(
    "--tmp",
    "base_temporary_directory",
    help=(
        "Create this run's temporary directory inside a non-default base directory. "
        "This overrides the configured base temporary directory without modifying saved config."
    ),
    required=False,
    type=rich_click.Path(writable=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--check",
    help=(
        "Only check whether any log files are pending extraction, then exit without extracting anything. "
        f"Exits with status {_CHECK_PENDING_EXIT_CODE} when some are pending and 0 when none are. "
        "The check stops at the first pending file, and writes no records and no temporary directory."
    ),
    is_flag=True,
    default=False,
)
def _extract_cli(
    directory: str,
    limit: int | None = None,
    workers: int = -2,
    cache_directory: str | None = None,
    mode: typing.Literal["remote"] | None = None,
    inventory_directory: str | None = None,
    use_encryption: bool = True,
    base_temporary_directory: str | None = None,
    check: bool = False,
) -> None:
    """
    Extract S3 log access data from the specified directory.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.

    Requests from the addresses listed in '~/.s3-log-extraction/excluded_ips.txt' are dropped and never enter the
    cache.
    Removing an address from that file does not bring back its requests. To include it again, run
    'reset extraction' and then a full 'extract'.

    DIRECTORY : The path to the folder containing all raw S3 log files.
    """
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else None
    base_temporary_path = pathlib.Path(base_temporary_directory) if base_temporary_directory is not None else None
    match mode:
        case "remote":
            extractor = RemoteS3LogAccessExtractor(
                cache_directory=cache_path,
                use_encryption=use_encryption,
                base_temporary_directory=base_temporary_path,
            )
            if check is True:
                is_pending = extractor.has_unprocessed_s3_urls(
                    s3_root=directory, inventory_directory=inventory_directory
                )
                _report_check(is_pending)
                return

            extractor.extract_s3_bucket(
                s3_root=directory,
                limit=limit,
                workers=workers,
                inventory_directory=inventory_directory,
            )
        case _:
            extractor = S3LogAccessExtractor(
                cache_directory=cache_path,
                use_encryption=use_encryption,
                base_temporary_directory=base_temporary_path,
            )
            if check is True:
                is_pending = extractor.has_unextracted_files(directory=directory)
                _report_check(is_pending)
                return

            extractor.extract_directory(directory=directory, limit=limit, workers=workers)


# s3logextraction stop
@s3logextraction_cli.command(name="stop")
@rich_click.option(
    "--timeout",
    "max_timeout_in_seconds",
    help=(
        "The maximum time to wait (in seconds) for the extraction processes to stop before "
        "ceasing to track their status. This does not mean that the processes will not stop after this time."
        "Recall this command to start a new timeout."
    ),
    required=False,
    type=rich_click.IntRange(min=1),
    default=600,  # 10 minutes
)
@_cache_directory_option
def _stop_extraction_cli(max_timeout_in_seconds: int = 600, cache_directory: str | None = None) -> None:
    """
    Stop the extraction processes if any are currently running in other windows.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.
    """
    stop_extraction(
        cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None,
        max_timeout_in_seconds=max_timeout_in_seconds,
    )


# s3logextraction config
@s3logextraction_cli.group(name="config")
def _config_cli() -> None:
    """Configuration options, such as cache management."""


# s3logextraction config cache
@_config_cli.group(name="cache")
def _cache_cli() -> None:
    pass


# s3logextraction config cache set < directory >
@_cache_cli.command(name="set")
@rich_click.argument("directory", type=rich_click.Path(writable=True))
def _set_cache_cli(directory: str) -> None:
    """
    Set a non-default location for the cache directory.

    DIRECTORY : The path to the folder where the cache will be stored.
        The extraction cache typically uses 0.3% of the total size of the S3 logs being processed for simple files.
            For example, 20 GB of extracted data from 6 TB of logs.

        This amount is known to exceed 1.2% of the total size of the S3 logs being processed for Zarr stores.
            For example, 80 GB if extracted data from 6 TB of logs.
    """
    set_cache_directory(directory=directory)


# s3logextraction config tmp
@_config_cli.group(name="tmp")
def _tmp_cli() -> None:
    pass


# s3logextraction config tmp set < directory >
@_tmp_cli.command(name="set")
@rich_click.argument("directory", type=rich_click.Path(writable=True))
def _set_tmp_cli(directory: str) -> None:
    """
    Set a non-default base directory for the temporary directories of extraction runs.

    DIRECTORY : The path to the folder that each extraction run creates its temporary directory inside.
        Without this setting, runs use the system temporary directory, which on many systems is a
        small RAM-backed '/tmp' that a large extraction can exhaust.

        Parallel runs write their per-worker output here before each batch is merged into the cache, and
        remote runs also download each raw log file here. Size it for a batch of logs and prefer a fast
        local disk.
    """
    set_base_temporary_directory(directory)


# s3logextraction config tmp reset
@_tmp_cli.command(name="reset")
def _reset_tmp_cli() -> None:
    """Forget the configured base temporary directory and go back to using the system temporary directory."""
    unset_base_temporary_directory()


# s3logextraction reset
@s3logextraction_cli.group(name="reset")
def _reset_cli() -> None:
    pass


# s3logextraction reset extraction
@_reset_cli.command(name="extraction")
@_cache_directory_option
def _reset_extraction_cli(cache_directory: str | None = None) -> None:
    reset_extraction(cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None)


# s3logextraction reset excluded
@_reset_cli.command(name="excluded")
@rich_click.option(
    "--cache",
    "cache_directory",
    help=(
        "Clean a non-default cache directory. "
        "This overrides the configured cache directory without modifying saved config."
    ),
    required=False,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Decrypt and re-encrypt IP addresses in the extraction cache. Enabled by default.",
    type=rich_click.BOOL,
    default=True,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc."
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@rich_click.option(
    "--confirm",
    help="Actually remove the records. Without it, the records that would be removed are only listed.",
    is_flag=True,
    default=False,
)
def _reset_excluded_cli(
    cache_directory: str | None = None,
    use_encryption: bool = True,
    workers: int = -2,
    confirm: bool = False,
) -> None:
    """
    Remove the records made by an excluded IP address from the extraction cache.

    These are the addresses listed in '~/.s3-log-extraction/excluded_ips.txt'. Their records leave every summary
    generated afterwards.

    The removal is permanent. Taking an address off the excluded list later does not bring its records back, so
    un-excluding one requires 'reset extraction' followed by a new 'extract'.

    Without '--confirm', nothing is changed, and the object keys whose records would be removed are listed instead.
    The cache must not be extracted to at the same time, so back it up and hold the extraction lock first.
    Each affected object key is listed with its record count. The addresses themselves are not printed.
    """
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else None
    excluded_ips = get_excluded_ips()
    if not excluded_ips:
        rich_click.echo("No excluded IP addresses are configured, so there is nothing to remove.")
        return

    if not confirm:
        records = find_excluded_ip_records(
            cache_directory=cache_path, excluded_ips=excluded_ips, use_encryption=use_encryption, workers=workers
        ).records
    else:
        records = remove_excluded_ip_records(
            cache_directory=cache_path, excluded_ips=excluded_ips, use_encryption=use_encryption, workers=workers
        )
    if not records:
        rich_click.echo(f"No records of the {len(excluded_ips)} excluded IP addresses were found.")
        return

    for object_key, record_counts_per_ip in records.items():
        rich_click.echo(f"{object_key}: {sum(record_counts_per_ip.values())} records")
    record_count = sum(count for record_counts_per_ip in records.values() for count in record_counts_per_ip.values())
    found_ip_count = len({ip for record_counts_per_ip in records.values() for ip in record_counts_per_ip})
    verb = "Removed" if confirm else "Would remove"
    rich_click.echo(
        f"{verb} {record_count} records of {found_ip_count} excluded IP addresses "
        f"across {len(records)} object keys of the extraction cache."
    )
    if not confirm:
        rich_click.echo("Nothing was changed. Run again with '--confirm' to remove them.")


# s3logextraction update
@s3logextraction_cli.group(name="update")
def _update_cli() -> None:
    pass


# s3logextraction update ip
@_update_cli.group(name="ip")
def _update_ip_cli() -> None:
    pass


# s3logextraction update ip database
@_update_ip_cli.command(name="database")
@_cache_directory_option
@rich_click.option(
    "--force",
    help="Download a fresh copy even if the cached database is not yet stale.",
    is_flag=True,
    default=False,
)
def _update_ip_database_cli(cache_directory: str | None = None, force: bool = False) -> None:
    """
    Download the MaxMind GeoLite2-City database used to geolocate IP addresses, if missing or stale.

    Requires the MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY environment variables of a (free) MaxMind account.
    The `summaries` command runs this automatically, so it is only needed to force a refresh.
    """
    database_path = update_geolite2_database(
        cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None,
        force=force,
    )
    print(f"GeoLite2 database is up to date at {database_path}")


# s3logextraction update ip coordinates
@_update_ip_cli.command(name="coordinates")
@_cache_directory_option
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Encrypt/decrypt the coordinates cache file. Enabled by default.",
    type=rich_click.BOOL,
    default=True,
)
def _update_ip_coordinates_cli(cache_directory: str | None = None, use_encryption: bool = True) -> None:
    """
    Locate every region label of the published by-region summaries, writing `region_codes_to_coordinates.yaml`.

    Run after `update summaries`. Geographic labels are looked up in the ISO 3166 tables bundled with the package;
    cloud service regions are located with the GeoLite2 database.
    """
    update_region_code_coordinates(
        cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None,
        use_encryption=use_encryption,
    )


# s3logextraction update summaries
@_update_cli.command(name="summaries")
@rich_click.option(
    "--mode",
    help=(
        "Generate condensed summaries of activity across the extracted data per object key. "
        "Defaults to grouping summaries by top level prefix."
        "Mode 'archive' aggregates over all dataset summaries."
    ),
    required=False,
    type=rich_click.Choice(choices=["archive"]),
    default=None,
)
@rich_click.option(
    "--pick",
    help="A comma-separated list of directories to exclusively select when generating summaries.",
    required=False,
    type=rich_click.STRING,
    default=None,
)
@rich_click.option(
    "--skip",
    help="A comma-separated list of directories to exclude when generating summaries.",
    required=False,
    type=rich_click.STRING,
    default=None,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc. "
        "By default, "
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@rich_click.option(
    "--asset-types-in-order",
    help="Archive mode only: comma-separated list of known asset types used for output column ordering (no spaces).",
    required=False,
    type=rich_click.STRING,
    default=None,
)
@rich_click.option(
    "--threshold",
    "region_disclosure_threshold",
    help=(
        "The number of resolved regions an update to a 'by_region.tsv' must move at once for it to be published. "
        "Below this, the summary is left as it was, so that no single requester's activity can be read off "
        "the change. A resolved region is any label naming a physical place, such as 'USA/CA'."
    ),
    required=False,
    type=rich_click.IntRange(min=0),
    default=REGION_DISCLOSURE_THRESHOLD,
    show_default=True,
)
@_cache_directory_option
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Decrypt IP addresses in the extraction cache. Enabled by default.",
    type=rich_click.BOOL,
    default=True,
)
def _update_summaries_cli(
    mode: typing.Literal["archive"] | None = None,
    pick: str | None = None,
    skip: str | None = None,
    workers: int = -2,
    asset_types_in_order: str | None = None,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
    cache_directory: str | None = None,
    use_encryption: bool = True,
) -> None:
    """
    Generate condensed summaries of activity.

    Requesters are geolocated while the summaries are generated, against the published IP ranges of known cloud
    services and VPNs and the local GeoLite2 database. The database is downloaded on first use and refreshed once
    a week old, which requires the MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY environment variables.

    Any addresses listed in '~/.s3-log-extraction/excluded_ips.txt' are left out of every summary. Requesters in the
    published GitHub ranges are left out of the view and requester counts only.
    Removing an address from that file does not bring back requests that extraction already dropped. To include it
    again, run 'reset extraction' and then a full 'extract'.
    """
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else None
    match mode:
        case "archive":
            parsed_asset_types_in_order = asset_types_in_order.split(",") if asset_types_in_order is not None else None
            generate_archive_summaries(
                cache_directory=cache_path,
                asset_types_in_order=parsed_asset_types_in_order,
                region_disclosure_threshold=region_disclosure_threshold,
            )
        case _:
            generate_summaries(
                cache_directory=cache_path,
                use_encryption=use_encryption,
                region_disclosure_threshold=region_disclosure_threshold,
            )


# s3logextraction update totals
@_update_cli.command(name="totals")
@rich_click.option(
    "--mode",
    help="Generate condensed summaries of activity across the extracted data per object key.",
    required=False,
    type=rich_click.Choice(choices=["archive"]),
    default=None,
)
@_cache_directory_option
def _update_totals_cli(
    mode: typing.Literal["archive"] | None = None,
    cache_directory: str | None = None,
) -> None:
    """Generate grand totals of all extracted data."""
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else None
    match mode:
        case "archive":
            generate_archive_totals(cache_directory=cache_path)
        case _:
            generate_all_dataset_totals(cache_directory=cache_path)


# s3logextraction testing
@s3logextraction_cli.group(name="testing")
def _testing_cli() -> None:
    """Testing utilities for the S3 log extraction."""


# s3logextraction testing generate benchmark
@_testing_cli.group(name="generate")
def _testing_generate_cli() -> None:
    """Generate various types of mock data for testing purposes."""


# s3logextraction testing generate benchmark
@_testing_generate_cli.command(name="benchmark")
@rich_click.argument("directory", type=rich_click.Path(writable=True))
def _generate_benchmark_cli(directory: str) -> None:
    """
    Generate a ~120 MB benchmark of the S3 log extraction to use for performance testing.

    DIRECTORY : The path to the folder where the benchmark will be stored.
    """
    generate_benchmark(directory=directory)


# s3logextraction validate
@s3logextraction_cli.group(name="validate")
def _validate_cli() -> None:
    """Validate the raw logs with a pre-validation protocol, or the extraction cache against the excluded IPs."""


def _add_pre_validator_command(*, protocol: str, validator_class: type) -> None:
    """Register ``s3logextraction validate <protocol> <directory>``, taking its help text from the validator."""

    def _pre_validator_cli(directory: str) -> None:
        validator = validator_class()
        validator.validate_directory(directory=directory)

    summary = (inspect.getdoc(validator_class) or "").split("\n\n", 1)[0]
    help_text = f"{summary}\n\nDIRECTORY : The path to the folder containing the raw S3 log files to validate."
    directory_argument = rich_click.argument("directory", type=rich_click.Path(writable=False))
    _validate_cli.command(name=protocol, help=help_text)(directory_argument(_pre_validator_cli))


# s3logextraction validate < protocol > < directory >
for _protocol, _validator_class in _PRE_VALIDATORS.items():
    _add_pre_validator_command(protocol=_protocol, validator_class=_validator_class)


# s3logextraction validate excluded
@_validate_cli.command(name="excluded")
@rich_click.option(
    "--cache",
    "cache_directory",
    help=(
        "Scan a non-default cache directory. "
        "This overrides the configured cache directory without modifying saved config."
    ),
    required=False,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Decrypt IP addresses in the extraction cache. Enabled by default.",
    type=rich_click.BOOL,
    default=True,
)
@rich_click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc."
    ),
    required=False,
    type=rich_click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
def _validate_excluded_cli(
    cache_directory: str | None = None,
    use_encryption: bool = True,
    workers: int = -2,
) -> None:
    """
    Scan the extraction cache for records made by an excluded IP address.

    These are the addresses listed in '~/.s3-log-extraction/excluded_ips.txt'. Records extracted before an address
    was excluded stay in the cache until they are removed.

    The number of object keys scanned is printed first, so that a run shows how much of the cache it covered.
    Each object key with such records is then listed with its record count. The addresses themselves are not printed.
    The command exits with an error when any record is found, or when a directory of the cache cannot be read, so
    that a scheduled job running it fails.
    """
    excluded_ips = get_excluded_ips()
    if not excluded_ips:
        rich_click.echo("No excluded IP addresses are configured, so there is nothing to scan for.")
        return

    scan = find_excluded_ip_records(
        cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None,
        excluded_ips=excluded_ips,
        use_encryption=use_encryption,
        workers=workers,
    )
    records = scan.records
    rich_click.echo(f"Scanned {scan.object_key_count} object keys of the extraction cache.")
    if not records:
        rich_click.echo(f"No records of the {len(excluded_ips)} excluded IP addresses were found.")
        return

    for object_key, record_counts_per_ip in records.items():
        rich_click.echo(f"{object_key}: {sum(record_counts_per_ip.values())} records")
    record_count = sum(count for record_counts_per_ip in records.values() for count in record_counts_per_ip.values())
    found_ip_count = len({ip for record_counts_per_ip in records.values() for ip in record_counts_per_ip})
    message = (
        f"Found {record_count} records of {found_ip_count} excluded IP addresses "
        f"across {len(records)} object keys of the extraction cache."
    )
    raise rich_click.ClickException(message)


# s3logextraction stats --inventory <path> [--cache <path>]
@s3logextraction_cli.command(name="stats")
@rich_click.option(
    "--inventory",
    "inventory_directory",
    help=(
        "Path to a local pre-downloaded AWS S3 Inventory directory. "
        "The directory must contain a 'hive/' sub-folder with Hive-partitioned symlink files "
        "(e.g. hive/dt=YYYY-MM-DD-HH-MM/symlink.txt), a 'data/' sub-folder with the "
        "gzip-compressed CSV inventory files, and timestamped manifest directories "
        "(e.g. 2026-05-03T01-00Z/manifest.json)."
    ),
    required=True,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
)
@rich_click.option(
    "--cache",
    "cache_directory",
    help=(
        "Optional cache directory containing the extraction cache and the GeoLite2 database. "
        "If omitted, uses the configured default cache directory."
    ),
    required=False,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
    default=None,
)
@rich_click.option(
    "--encryption",
    "use_encryption",
    help="Decrypt IP addresses in the extraction cache. Enabled by default; pass --encryption=false for plaintext.",
    type=rich_click.BOOL,
    default=True,
)
def _stats_cli(inventory_directory: str, cache_directory: str | None = None, use_encryption: bool = True) -> None:
    """
    Report log-file inventory stats, extraction completion, and IP address classification stats.

    Reads a local pre-downloaded AWS S3 Inventory directory and prints the
    file count for all objects in the inventory, the extraction completion
    percentage, and a breakdown of how the extracted IP addresses classify
    (determined, unknown, bogon, VPN, cloud service, GitHub), resolved the same
    way the summaries resolve them.
    """
    inventory_path = pathlib.Path(inventory_directory)
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else None

    stats = get_log_bucket_stats(inventory_directory=inventory_path)
    rich_click.echo(f"File count      : {stats['file_count']}")

    completion = get_extraction_completion(
        inventory_directory=inventory_path,
        cache_directory=cache_path,
    )
    rich_click.echo(f"Processed files : {completion['processed_file_count']}")
    rich_click.echo(f"Percent complete: {completion['percent_complete']:.2f}%")

    ip_stats = get_ip_stats(cache_directory=cache_path, use_encryption=use_encryption)

    rich_click.echo("")
    rich_click.echo(f"Extracted IPs   : {ip_stats['extracted_ip_count']}")

    rows: list[tuple[str, IpCategoryCount]] = [
        ("Determined", ip_stats["determined"]),
        ("Unknown", ip_stats["unknown"]),
        ("Bogon", ip_stats["bogon"]),
        ("VPN", ip_stats["vpn"]),
        ("Cloud service", ip_stats["cloud_service"]),
        ("GitHub", ip_stats["github"]),
    ]
    for label, entry in rows:
        rich_click.echo(f"  {label:<14}: {entry['count']:>7}  ({entry['percent']:6.2f}%)")


# s3logextraction completion --inventory <path> [--cache <path>]
@s3logextraction_cli.command(name="completion")
@rich_click.option(
    "--inventory",
    "inventory_directory",
    help=(
        "Path to a local pre-downloaded AWS S3 Inventory directory. "
        "The directory must contain a 'hive/' sub-folder with Hive-partitioned symlink files "
        "(e.g. hive/dt=YYYY-MM-DD-HH-MM/symlink.txt), a 'data/' sub-folder with the "
        "gzip-compressed CSV inventory files, and timestamped manifest directories "
        "(e.g. 2026-05-03T01-00Z/manifest.json)."
    ),
    required=True,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
)
@rich_click.option(
    "--cache",
    "cache_directory",
    help=(
        "Optional cache directory containing extraction records. "
        "If omitted, uses the configured default cache directory."
    ),
    required=False,
    type=rich_click.Path(exists=True, file_okay=False, dir_okay=True),
    default=None,
)
def _completion_cli(inventory_directory: str, cache_directory: str | None = None) -> None:
    """
    Report extraction completion percentage from end records vs latest inventory.

    The command compares the number of unique entries in the remote extraction
    end record against the current inventory file count.
    """
    completion = get_extraction_completion(
        inventory_directory=pathlib.Path(inventory_directory),
        cache_directory=pathlib.Path(cache_directory) if cache_directory is not None else None,
    )
    rich_click.echo(f"Processed files  : {completion['processed_file_count']}")
    rich_click.echo(f"Inventory files  : {completion['inventory_file_count']}")
    rich_click.echo(f"Percent complete : {completion['percent_complete']:.2f}%")

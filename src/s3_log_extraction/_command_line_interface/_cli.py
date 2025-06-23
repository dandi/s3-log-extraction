"""Call the DANDI S3 log parser from the command line."""

import os
import typing

import click

from ..config import reset_extraction, set_cache_directory
from ..extractors import DandiS3LogAccessExtractor, RemoteS3LogAccessExtractor, S3LogAccessExtractor, stop_extraction
from ..ip_utils import index_ips, update_index_to_region_codes, update_region_code_coordinates
from ..summarize import (
    generate_all_dandiset_summaries,
    generate_all_dandiset_totals,
    generate_archive_summaries,
    generate_archive_totals,
)
from ..testing import generate_benchmark


# s3logextraction
@click.group()
def _s3logextraction_cli():
    pass


# s3logextraction extract < directory >
@_s3logextraction_cli.command(name="extract")
@click.argument("directory", type=click.Path(writable=False))
@click.option(
    "--limit",
    help="The maximum number of files to process. By default, all files will be processed.",
    required=False,
    type=click.IntRange(min=1),
    default=None,
)
@click.option(
    "--workers",
    help=(
        "The maximum number of workers to use for parallel processing. "
        "Allows negative slicing semantics, where -1 means all available cores, -2 means all but one, etc. "
        "By default, "
    ),
    required=False,
    type=click.IntRange(min=-os.cpu_count() + 1, max=os.cpu_count()),
    default=-2,
)
@click.option(
    "--mode",
    help=(
        "Special parsing mode related to expected object key structure; "
        "for example, if 'dandi' then only extract 'blobs' and 'zarr' objects. "
        "By default, objects will be processed using the generic structure."
    ),
    required=False,
    type=click.Choice(choices=["dandi", "dandi-remote"]),
    default=None,
)
@click.option(
    "--manifest",
    "manifest_file_path",
    help=(
        "A custom manifest file specifying the paths of log files to process from the S3 bucket that would not be "
        "discovered by the natural nesting pattern. Typically used in cases where the storage pattern was swapped "
        "from flat to nested at a particular point in time."
    ),
    required=False,
    type=click.Path(writable=False),
    default=None,
)
def _extract_cli(
    directory: str,
    limit: int | None = None,
    workers: int = -2,
    mode: typing.Literal["dandi", "dandi-remote"] | None = None,
    manifest_file_path: str | None = None,
) -> None:
    """
    Extract S3 log access data from the specified directory.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.

    DIRECTORY : The path to the folder containing all raw S3 log files.
    """
    match mode:
        case "dandi":
            extractor = DandiS3LogAccessExtractor()
            extractor.extract_directory(directory=directory, limit=limit, workers=workers)
        case "dandi-remote":
            extractor = RemoteS3LogAccessExtractor()
            extractor.extract_s3(
                s3_url=directory,
                date_limit=limit,
                file_limit=limit,
                workers=workers,
                mode="dandi",
                manifest_file_path=manifest_file_path,
            )
        case _:
            extractor = S3LogAccessExtractor()
            extractor.extract_directory(directory=directory, limit=limit, workers=workers)


# s3logextraction stop
@_s3logextraction_cli.command(name="stop")
@click.option(
    "--timeout",
    "max_timeout_in_seconds",
    help=(
        "The maximum time to wait (in seconds) for the extraction processes to stop before "
        "ceasing to track their status. This does not mean that the processes will not stop after this time."
        "Recall this command to start a new timeout."
    ),
    required=False,
    type=click.IntRange(min=1),
    default=600,  # 10 minutes
)
def _stop_extraction_cli(max_timeout_in_seconds: int = 600) -> None:
    """
    Stop the extraction processes if any are currently running in other windows.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.
    """
    stop_extraction(max_timeout_in_seconds=max_timeout_in_seconds)


# s3logextraction config
@_s3logextraction_cli.group(name="config")
def _config_cli() -> None:
    """Configuration options, such as cache management."""
    pass


# s3logextraction config cache
@_config_cli.group(name="cache")
def _cache_cli() -> None:
    pass


# s3logextraction config cache set < directory >
@_cache_cli.command(name="set")
@click.argument("directory", type=click.Path(writable=True))
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


# s3logextraction reset
@_s3logextraction_cli.group(name="reset")
def _reset_cli() -> None:
    pass


# s3logextraction reset extraction
@_reset_cli.command(name="extraction")
def _reset_extraction_cli() -> None:
    reset_extraction()


# s3logextraction update
@_s3logextraction_cli.group(name="update")
def _update_cli() -> None:
    pass


# s3logextraction update ip
@_update_cli.group(name="ip")
def _update_ip_cli() -> None:
    pass


# s3logextraction update ip indexes
@_update_ip_cli.command(name="indexes")
def _update_ip_indexes_cli() -> None:
    index_ips()


# s3logextraction update ip regions
@_update_ip_cli.command(name="regions")
def _update_ip_regions_cli() -> None:
    update_index_to_region_codes()


# s3logextraction update ip coordinates
@_update_ip_cli.command(name="coordinates")
def _update_ip_coordinates_cli() -> None:
    update_region_code_coordinates()


# s3logextraction update summaries
@_update_cli.command(name="summaries")
@click.option(
    "--mode",
    help=(
        "Generate condensed summaries of activity across the extracted data per object key. "
        "Mode 'dandi' will map asset hashes to Dandisets and their content filenames. "
        "Mode 'archive' aggregates over all dataset summaries."
    ),
    required=False,
    type=click.Choice(choices=["dandi", "archive"]),
    default=None,
)
def _update_summaries_cli(mode: typing.Literal["dandi", "archive"] | None = None) -> None:
    """
    Generate condensed summaries of activity.

    TODO
    """
    match mode:
        case "dandi":
            generate_all_dandiset_summaries()
        case "archive":
            generate_archive_summaries()
        case _:
            message = "The generic mode is not yet implemented - please raise an issue to discuss."
            click.echo(message=message, err=True)


# s3logextraction update totals
@_update_cli.command(name="totals")
@click.option(
    "--mode",
    help=(
        "Generate condensed summaries of activity across the extracted data per object key. "
        "Mode 'dandi' will map asset hashes to Dandisets and their content filenames. "
    ),
    required=False,
    type=click.Choice(choices=["dandi", "archive"]),
    default=None,
)
def _update_totals_cli(mode: typing.Literal["dandi", "archive"] | None = None) -> None:
    """Generate grand totals of all extracted data."""
    match mode:
        case "dandi":
            generate_all_dandiset_totals()
        case "archive":
            generate_archive_totals()
        case _:
            message = "The generic mode is not yet implemented - please raise an issue to discuss."
            click.echo(message=message, err=True)


# s3logextraction testing
@_s3logextraction_cli.group(name="testing")
def _testing_cli() -> None:
    """Testing utilities for the S3 log extraction."""
    pass


# s3logextraction testing generate benchmark
@_testing_cli.group(name="generate")
def _testing_generate_cli() -> None:
    """Generate various types of mock data for testing purposes."""
    pass


# s3logextraction testing generate benchmark
@_testing_generate_cli.command(name="benchmark")
@click.argument("directory", type=click.Path(writable=True))
def _generate_benchmark_cli(directory: str) -> None:
    """
    Generate a ~120 MB benchmark of the S3 log extraction to use for performance testing.

    DIRECTORY : The path to the folder where the benchmark will be stored.
    """
    generate_benchmark(directory=directory)

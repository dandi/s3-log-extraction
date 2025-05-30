"""Call the DANDI S3 log parser from the command line."""

import collections
import pathlib
import sys

import click

from ._bin_all_reduced_s3_logs_by_object_key import bin_all_reduced_s3_logs_by_object_key
from ._dandi_s3_log_file_reducer import (
    reduce_all_dandi_raw_s3_logs,
)
from ._generate_all_dandiset_totals import generate_all_dandiset_totals
from ._generate_archive_summaries import generate_archive_summaries
from ._generate_archive_totals import generate_archive_totals
from ._map_binned_s3_logs_to_dandisets import map_binned_s3_logs_to_dandisets
from ._update_region_codes_to_coordinates import update_region_codes_to_coordinates


@click.command(name="reduce_all_dandi_raw_s3_logs")
@click.option(
    "--raw_s3_logs_folder_path",
    help="The path to the folder containing all raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--reduced_s3_logs_folder_path",
    help="The path to write each reduced S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
@click.option(
    "--maximum_number_of_workers",
    help="The maximum number of workers to distribute tasks across.",
    required=False,
    type=click.IntRange(min=1),
    default=1,
)
@click.option(
    "--maximum_buffer_size_in_mb",
    help=(
        "The theoretical maximum amount of RAM (in MB) to use on each buffer iteration when reading from the "
        "source text files. "
        "Actual total RAM usage will be higher due to overhead and caching. "
        "Automatically splits this total amount over the maximum number of workers if `maximum_number_of_workers` is "
        "greater than one."
    ),
    required=False,
    type=click.IntRange(min=1),  # Bare minimum of 1 MB
    default=1_000,  # 1 GB recommended
)
@click.option(
    "--excluded_years",
    help="A comma-separated list of years to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--excluded_ips",
    help="A comma-separated list of IP addresses to exclude from parsing.",
    required=False,
    type=str,
    default=None,
)
def _reduce_all_dandi_raw_s3_logs_cli(
    raw_s3_logs_folder_path: str,
    reduced_s3_logs_folder_path: str,
    maximum_number_of_workers: int,
    maximum_buffer_size_in_mb: int,
    excluded_years: str | None,
    excluded_ips: str | None,
) -> None:
    split_excluded_years = excluded_years.split(",") if excluded_years is not None else []
    split_excluded_ips = excluded_ips.split(",") if excluded_ips is not None else []
    handled_excluded_ips = collections.defaultdict(bool) if len(split_excluded_ips) != 0 else None
    for excluded_ip in split_excluded_ips:
        handled_excluded_ips[excluded_ip] = True
    maximum_buffer_size_in_bytes = maximum_buffer_size_in_mb * 10**6

    reduce_all_dandi_raw_s3_logs(
        raw_s3_logs_folder_path=raw_s3_logs_folder_path,
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        maximum_number_of_workers=maximum_number_of_workers,
        maximum_buffer_size_in_bytes=maximum_buffer_size_in_bytes,
        excluded_years=split_excluded_years,
        excluded_ips=handled_excluded_ips,
    )

    return None


@click.command(name="bin_all_reduced_s3_logs_by_object_key")
@click.option(
    "--reduced_s3_logs_folder_path",
    help="The path to the folder containing all raw S3 log files.",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--binned_s3_logs_folder_path",
    help="The path to write each reduced S3 log file to. There will be one file per handled asset ID.",
    required=True,
    type=click.Path(writable=True),
)
@click.option(
    "--file_limit",
    help="The maximum number of files to process per call.",
    required=False,
    type=int,
    default=None,
)
def _bin_all_reduced_s3_logs_by_object_key_cli(
    reduced_s3_logs_folder_path: str,
    binned_s3_logs_folder_path: str,
    file_limit: int | None,
) -> None:
    bin_all_reduced_s3_logs_by_object_key(
        reduced_s3_logs_folder_path=reduced_s3_logs_folder_path,
        binned_s3_logs_folder_path=binned_s3_logs_folder_path,
        file_limit=file_limit,
    )

    return None


@click.command(name="map_binned_s3_logs_to_dandisets")
@click.option(
    "--binned_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--excluded_dandisets",
    help="A comma-separated list of Dandiset IDs to exclude from processing.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--restrict_to_dandisets",
    help="A comma-separated list of Dandiset IDs to exclusively process.",
    required=False,
    type=str,
    default=None,
)
@click.option(
    "--dandiset_limit",
    help="The maximum number of Dandisets to process per call. Useful for quick testing.",
    required=False,
    type=int,
    default=None,
)
def _map_binned_s3_logs_to_dandisets_cli(
    binned_s3_logs_folder_path: pathlib.Path,
    mapped_s3_logs_folder_path: pathlib.Path,
    excluded_dandisets: str | None,
    restrict_to_dandisets: str | None,
    dandiset_limit: int | None,
) -> None:
    split_excluded_dandisets = excluded_dandisets.split(",") if excluded_dandisets is not None else None
    split_restrict_to_dandisets = restrict_to_dandisets.split(",") if restrict_to_dandisets is not None else None

    map_binned_s3_logs_to_dandisets(
        binned_s3_logs_folder_path=binned_s3_logs_folder_path,
        mapped_s3_logs_folder_path=mapped_s3_logs_folder_path,
        excluded_dandisets=split_excluded_dandisets,
        restrict_to_dandisets=split_restrict_to_dandisets,
        dandiset_limit=dandiset_limit,
    )

    return None


@click.command(name="generate_dandiset_summaries")
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
def _generate_dandiset_summaries_cli(
    mapped_s3_logs_folder_path: pathlib.Path,
) -> None:
    raise NotImplementedError("This function is not implemented yet.")


@click.command(name="generate_all_dandiset_totals")
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
def _generate_all_dandiset_totals_cli(mapped_s3_logs_folder_path: pathlib.Path) -> None:
    generate_all_dandiset_totals(mapped_s3_logs_folder_path=mapped_s3_logs_folder_path)


@click.command(name="generate_archive_summaries")
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
def _generate_archive_summaries_cli(mapped_s3_logs_folder_path: pathlib.Path) -> None:
    generate_archive_summaries(mapped_s3_logs_folder_path=mapped_s3_logs_folder_path)


@click.command(name="generate_archive_totals")
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
def _generate_archive_totals_cli(mapped_s3_logs_folder_path: pathlib.Path) -> None:
    generate_archive_totals(mapped_s3_logs_folder_path=mapped_s3_logs_folder_path)


@click.command(name="update_region_codes_to_coordinates")
@click.option(
    "--mapped_s3_logs_folder_path",
    help="",
    required=True,
    type=click.Path(writable=False),
)
@click.option(
    "--cache_directory",
    help="",
    required=False,
    type=click.Path(writable=True),
)
@click.option(
    "--maximum_iterations",
    help="Maximum number of region codes to update.",
    required=False,
    type=int,
    default=None,
)
def _update_region_codes_to_coordinates_cli(
    mapped_s3_logs_folder_path: str,
    cache_directory: str | None = None,
    maximum_iterations: int | None = None,
) -> None:
    update_region_codes_to_coordinates(
        mapped_s3_logs_folder_path=mapped_s3_logs_folder_path,
        cache_directory=cache_directory,
        maximum_iterations=maximum_iterations,
    )


@click.command(name="check_for_errors")
@click.option(
    "--cache_directory",
    help="",
    required=False,
    type=click.Path(writable=True),
)
def _check_for_errors(cache_directory: str | pathlib.Path | None) -> int:
    cache_directory = pathlib.Path(cache_directory) if cache_directory is not None else pathlib.Path.home() / ".cache"
    cache_directory.mkdir(exist_ok=True)
    log_parser_cache_directory = cache_directory / "dandi_s3_log_parser"
    log_parser_cache_directory.mkdir(exist_ok=True)

    region_code_to_coordinates_error_directory = log_parser_cache_directory / "region_codes_to_coordinates_errors"
    region_code_to_coordinates_error_directory.mkdir(exist_ok=True)

    if len(list(region_code_to_coordinates_error_directory.iterdir())) > 0:
        click.echo(message="Region code to coordinate process resulted in errors - please investigate.", err=True)
        return sys.exit(1)

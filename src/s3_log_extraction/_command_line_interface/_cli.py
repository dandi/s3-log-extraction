"""Call the DANDI S3 log parser from the command line."""

import os
import time
import typing

import click

from ..config import get_extraction_directory, reset_extraction, reset_tmp, set_cache_directory
from ..extractors import DandiS3LogAccessExtractor, S3LogAccessExtractor, get_possible_running_pids


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
    help="The maximum number of workers to distribute tasks across. By default, only one worker will be used.",
    required=False,
    type=click.IntRange(min=1, max=os.cpu_count()),
    default=1,
)
@click.option(
    "--mode",
    help=(
        "Special parsing mode related to expected object key structure; "
        "for example, if 'dandi' then only extract 'blobs' and 'zarr' objects."
        "By default, objects will be processed using the generic structure."
    ),
    required=False,
    type=click.Choice(choices=["dandi"]),
    default=None,
)
def _extract_cli(
    directory: str, workers: int, limit: int | None = None, mode: typing.Literal["dandi"] | None = None
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
        case _:
            extractor = S3LogAccessExtractor()

    try:
        extractor.extract_directory(directory=directory, limit=limit, max_workers=workers)
    except KeyboardInterrupt:
        click.echo(
            message=(
                "In order to safely interrupt this process, "
                "please open a separate console in the environment and call `s3logextraction stop`."
            )
        )


# s3logextraction stop
@_s3logextraction_cli.command(name="stop")
@click.option(
    "--timeout",
    help=(
        "The maximum time to wait (in seconds) for the extraction processes to stop before "
        "ceasing to track their status. This does not mean that the processes will not stop after this time."
        "Recall this command to start a new timeout."
    ),
    required=False,
    type=click.IntRange(min=1),
    default=600,  # Default to 10 minutes
)
def _stop_extraction_cli(max_timeout_in_seconds: int = 600) -> None:
    """
    Stop the extraction processes if any are currently running in other windows.

    Note that you should not attempt to interrupt the extraction process using Ctrl+C or pkill, as this may lead to
    incomplete data extraction. Instead, use this command to safely stop the extraction process.
    """
    possible_running_pids = get_possible_running_pids()
    if len(possible_running_pids) == 0:
        click.echo(message="No extraction processes are currently running.")
        return

    pid_string = (
        f" on PIDs [{", ".join(possible_running_pids)}]"
        if len(possible_running_pids) > 1
        else f" on PID {possible_running_pids[0]}"
    )

    click.echo(message=f"Stopping the extraction process{pid_string}...")
    extraction_directory = get_extraction_directory()
    stop_file_path = extraction_directory / "stop_extraction"
    stop_file_path.touch()

    time_so_far_in_seconds = 0
    while time_so_far_in_seconds < max_timeout_in_seconds:
        if any(get_possible_running_pids()):
            time.sleep(1)
        else:
            click.echo(message="Extraction has been stopped.")
            stop_file_path.unlink(missing_ok=True)
            return

    click.echo(message="Tracking of process stoppage has timed out - please try calling the method again.")


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


# s3logextraction reset tmp
@_reset_cli.command(name="tmp")
def _reset_extraction_cli() -> None:
    reset_tmp()


# TODO:
# s3logextraction update
@_s3logextraction_cli.group(name="update")
def _update_cli() -> None:
    pass


# s3logextraction update ip
@_update_cli.group(name="ip")
def _ip_cli() -> None:
    pass


# s3logextraction update ip indexes
@_reset_cli.command(name="extraction")
def _update_ip_indexes_cli() -> None:
    click.echo(message="Updating IP indexes...")


# s3logextraction update ip regions
# s3logextraction update ip coordinates
# s3logextraction update dandiset summaries
# s3logextraction update dandiset totals
# s3logextraction update toplevel summaries
# s3logextraction update toplevel totals
# s3logextraction update archive summaries
# s3logextraction update archive totals

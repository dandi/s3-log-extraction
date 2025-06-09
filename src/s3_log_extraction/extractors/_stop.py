import pathlib

import psutil

from ..config import get_extraction_directory


def get_running_pids(cache_directory: str | pathlib.Path | None = None) -> list[str]:
    """
    Get a list of possible running PIDs from the temporary directory.

    This is used to identify which processes are currently running and may need to be stopped.
    """
    running_pids = {
        str(process.info["pid"]) for process in psutil.process_iter() if process.info["name"] == "s3logextraction"
    }
    return running_pids


def stop_extraction(cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Stop the extraction process by creating a stop file in the extraction directory.

    This allows multiple subprocesses to exit gracefully and in a semi-completed state to be resumed.
    """
    extraction_directory = get_extraction_directory(cache_directory=cache_directory)
    stop_file_path = extraction_directory / "stop_extraction"
    stop_file_path.touch()

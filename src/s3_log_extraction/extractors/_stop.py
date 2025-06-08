import pathlib

import psutil

from ..config import get_extraction_directory, get_temporary_directory


def get_running_pids(cache_directory: str | pathlib.Path | None = None) -> list[str]:
    """
    Get a list of possible running PIDs from the temporary directory.

    This is used to identify which processes are currently running and may need to be stopped.
    """
    temporary_directory = get_temporary_directory(cache_directory=cache_directory)
    possible_pids = {str(pid.name) for pid in temporary_directory.iterdir()}

    running_pids = {possible_pid for possible_pid in possible_pids if psutil.pid_exists(pid=int(possible_pid))}
    return running_pids


def stop_extraction(cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Stop the extraction process by creating a stop file in the extraction directory.

    This allows multiple subprocesses to exit gracefully and in a semi-completed state to be resumed.
    """
    extraction_directory = get_extraction_directory(cache_directory=cache_directory)
    stop_file_path = extraction_directory / "stop_extraction"
    stop_file_path.touch()

import os
import pathlib
import time

import psutil

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ..config import get_extraction_directory


def get_running_pids() -> list[str]:
    """
    Get a list of possible running PIDs from the temporary directory.

    This is used to identify which processes are currently running and may need to be stopped.
    """
    running_pids = {
        str(process.info["pid"])
        for process in psutil.process_iter(attrs=["name", "pid"])
        if process.info["name"] == "s3logextraction"
    } - {str(os.getpid())}
    return running_pids


def stop_extraction(cache_directory: str | pathlib.Path | None = None, max_timeout_in_seconds: int = 600) -> None:
    """
    Stop the extraction process by creating a stop file in the extraction directory.

    This allows multiple subprocesses to exit gracefully and in a semi-completed state to be resumed.
    """
    running_pids = get_running_pids()
    if len(running_pids) == 0:
        print("No extraction processes are currently running.")
        return

    pid_string = (
        f" on PIDs [{", ".join(running_pids)}]" if len(running_pids) > 1 else f" on PID {list(running_pids)[0]}"
    )

    print(f"Stopping the extraction process{pid_string}...")
    extraction_directory = get_extraction_directory(cache_directory=cache_directory)
    stop_file_path = extraction_directory / _STOP_EXTRACTION_FILE_NAME
    stop_file_path.touch()

    update_delay_in_seconds = 5
    time_so_far_in_seconds = 0
    while time_so_far_in_seconds < max_timeout_in_seconds:
        if any(get_running_pids()):
            time.sleep(update_delay_in_seconds)
            time_so_far_in_seconds += update_delay_in_seconds
        else:
            print("Extraction has been stopped.")
            stop_file_path.unlink(missing_ok=True)
            return

    print("Tracking of process stoppage has timed out - please try calling this method again.")

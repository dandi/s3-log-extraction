import collections
import itertools
import pathlib
import shutil

from ._config import get_extraction_directory, get_records_directory, get_temporary_directory


def reset_extraction(cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Clear and remake the extraction directory and clear related records.

    Note: clears the results and history of ALL extraction modes.
    """
    extraction_directory = get_extraction_directory(cache_directory=cache_directory)
    shutil.rmtree(path=extraction_directory)
    extraction_directory.mkdir(exist_ok=True)

    records_directory = get_records_directory(cache_directory=cache_directory)
    records = [
        record
        for record in itertools.chain(
            records_directory.glob("*_extraction.log"), records_directory.glob("*_file-processing-*.txt")
        )
    ]
    collections.deque((record.unlink(missing_ok=True) for record in records), maxlen=0)

    reset_tmp()


def reset_tmp(cache_directory: str | pathlib.Path | None = None) -> None:
    """Clear and remake the temporary directory."""
    base_temporary_directory = get_temporary_directory(cache_directory=cache_directory)
    shutil.rmtree(path=base_temporary_directory, ignore_errors=True)
    base_temporary_directory.mkdir(exist_ok=True)

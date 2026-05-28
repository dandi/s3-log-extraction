import collections
import itertools
import pathlib
import shutil

from ._config import get_cache_directory, get_cache_subdirectory


def reset_extraction(cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Clear and remake the extraction directory and clear related records.

    Note: clears the results and history of ALL extraction modes.
    """
    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_dir / "extraction"
    extraction_directory.mkdir(exist_ok=True)
    shutil.rmtree(path=extraction_directory)
    extraction_directory.mkdir(exist_ok=True)

    records_directory = get_cache_subdirectory(cache_directory=cache_directory, name="records")
    records = [
        record
        for record in itertools.chain(
            records_directory.glob("*_extraction.log"), records_directory.glob("*_file-processing-*.txt")
        )
    ]
    collections.deque((record.unlink(missing_ok=True) for record in records), maxlen=0)

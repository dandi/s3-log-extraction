import collections.abc
import functools
import pathlib

from ._excluded_ip_records import (
    _PER_REQUEST_FILE_NAMES,
    _iter_batches,
    _iter_object_key_directories,
    _map_batches,
    _scan_object_key_directory,
)
from ._ip_utils import _read_ips_from_file, _write_ips_to_file
from ..config import get_cache_directory, get_excluded_ips
from ..utils.parallel import _handle_max_workers

# Every extractor appends a key to a '*-processing-start.txt' record before it writes to the cache, and the same key
# to the matching '*-processing-end.txt' record once it is done
_START_RECORD_SUFFIX = "-processing-start.txt"
_END_RECORD_SUFFIX = "-processing-end.txt"

# Rewritten files are first written to a sibling with this suffix, then moved over the original
_TEMPORARY_SUFFIX = ".tmp"


def _find_unfinished_records(records_directory: pathlib.Path, /) -> list[str]:
    """Return the names of the start records holding a key that the matching end record does not."""
    unfinished_record_names = []
    for start_record_file_path in sorted(records_directory.glob(pattern=f"*{_START_RECORD_SUFFIX}")):
        end_record_file_path = start_record_file_path.with_name(
            start_record_file_path.name.removesuffix(_START_RECORD_SUFFIX) + _END_RECORD_SUFFIX
        )
        started_keys = set(start_record_file_path.read_text().splitlines())
        ended_keys = set(end_record_file_path.read_text().splitlines()) if end_record_file_path.exists() else set()
        if started_keys - ended_keys:
            unfinished_record_names.append(start_record_file_path.name)
    return unfinished_record_names


def _remove_from_object_key_directory(
    *,
    object_key_directory: pathlib.Path,
    excluded_ips: frozenset[str],
    use_encryption: bool,
) -> dict[str, int]:
    """
    Remove the records of one object key directory that were made by an excluded IP.

    Line N of every per-request file describes the same request, so the matching lines are dropped from all of them
    together. Every rewritten file is written in full to a temporary sibling before any original is replaced. When no
    record is left, the files are deleted instead, and the directory itself is left for the caller to prune.

    Returns
    -------
    dict of str to int
        The number of records removed for each excluded IP. Empty when the directory held none.
    """
    record_counts_per_ip = _scan_object_key_directory(
        object_key_directory=object_key_directory, excluded_ips=excluded_ips, use_encryption=use_encryption
    )
    if not record_counts_per_ip:
        return record_counts_per_ip

    ips = _read_ips_from_file(file_path=object_key_directory / "ips.txt", use_encryption=use_encryption)
    kept_line_indices = [index for index, ip in enumerate(ips) if ip not in excluded_ips]
    if not kept_line_indices:
        for file_name in _PER_REQUEST_FILE_NAMES:
            (object_key_directory / file_name).unlink()
        return record_counts_per_ip

    temporary_file_paths = {
        file_name: object_key_directory / f"{file_name}{_TEMPORARY_SUFFIX}" for file_name in _PER_REQUEST_FILE_NAMES
    }
    for file_name, temporary_file_path in temporary_file_paths.items():
        if file_name == "ips.txt":
            kept_ips = [ips[index] for index in kept_line_indices]
            _write_ips_to_file(file_path=temporary_file_path, ips=kept_ips, use_encryption=use_encryption)
            continue

        lines = [line for line in (object_key_directory / file_name).read_text().splitlines() if line.strip()]
        kept_lines = [lines[index] for index in kept_line_indices]
        temporary_file_path.write_text("".join(f"{line}\n" for line in kept_lines))
    for file_name, temporary_file_path in temporary_file_paths.items():
        temporary_file_path.replace(target=object_key_directory / file_name)

    return record_counts_per_ip


def _remove_batch(
    *,
    object_key_directories: list[pathlib.Path],
    extraction_directory: pathlib.Path,
    excluded_ips: frozenset[str],
    use_encryption: bool,
) -> dict[str, dict[str, int]]:
    """Remove the records of excluded IPs from a batch of object key directories, keeping only those with any."""
    batch_records: dict[str, dict[str, int]] = {}
    for object_key_directory in object_key_directories:
        record_counts_per_ip = _remove_from_object_key_directory(
            object_key_directory=object_key_directory, excluded_ips=excluded_ips, use_encryption=use_encryption
        )
        if record_counts_per_ip:
            object_key = object_key_directory.relative_to(extraction_directory).as_posix()
            batch_records[object_key] = record_counts_per_ip
    return batch_records


def _prune_empty_directories(*, directory: pathlib.Path, extraction_directory: pathlib.Path) -> None:
    """Delete a directory and each of its parents below the extraction directory, for as long as they are empty."""
    while directory != extraction_directory and not any(directory.iterdir()):
        directory.rmdir()
        directory = directory.parent


def remove_excluded_ip_records(
    *,
    cache_directory: str | pathlib.Path | None = None,
    excluded_ips: collections.abc.Iterable[str] | None = None,
    use_encryption: bool = True,
    workers: int = -2,
) -> dict[str, dict[str, int]]:
    """
    Remove the records of the extraction cache that were made by an excluded IP address.

    Extraction drops the requests of excluded addresses, but records extracted before an address was excluded stay
    in the cache. This removes them, so that they leave every summary generated from then on. Line N of each
    per-request file of an object key describes the same request, so the matching lines are removed from all four
    files together. An object key left without any record has its files and its directory deleted, along with any
    parent directory this leaves empty.

    The removal is permanent. Taking an address off the excluded list later does not bring its records back, so
    un-excluding one requires a full re-extraction with ``reset_extraction`` followed by a new extraction.

    Each rewritten file is written in full to a temporary sibling and then moved over the original, so a crash never
    leaves a partially written file. The files of one object key are still replaced one after another, so take a
    backup of the cache first. A crash between those replacements leaves the object key misaligned, which every later
    scan, removal, and summary refuses with an error rather than miscounting.

    The cache must not be written to while this runs. Removal refuses to start if an extraction record shows a log
    whose extraction started but never finished, which is the case both while an extraction is running and after
    one was interrupted. It cannot detect an extraction that starts after it does, so hold the same lock as
    extraction around it when both are scheduled.

    Parameters
    ----------
    cache_directory : path-like, optional
        The cache directory holding the ``extraction`` directory to clean.
        If not provided, the configured cache directory is used.
    excluded_ips : iterable of str, optional
        The addresses whose records are removed, in canonical text form.
        If not provided, the result of ``s3_log_extraction.config.get_excluded_ips()`` is used.
    use_encryption : bool, default: True
        Whether the ``ips.txt`` files of the cache are encrypted. Rewritten files keep the same form.
        Pass ``False`` for a cache written without encryption.
    workers : int, default: -2
        The maximum number of worker processes.
        Negative values count back from all available cores, so -1 means all of them and -2 all but one.
        A value of 1 works in the current process.

    Returns
    -------
    dict of str to dict of str to int
        For each object key that held any record of an excluded address, the number of its records removed for each
        such address. Object keys are paths relative to the ``extraction`` directory, and are sorted.
        An empty dictionary means nothing was removed.

    Raises
    ------
    FileNotFoundError
        If the cache directory holds no ``extraction`` directory.
    OSError
        If a directory of the cache cannot be listed, such as one the current user may not read. Object keys that
        were already cleaned when the error is raised stay cleaned.
    RuntimeError
        If an extraction record shows an unfinished extraction, if an object key directory is missing a per-request
        file, or if one holding a record of an excluded address is not line-aligned. Object keys that were already
        cleaned when the error is raised stay cleaned, and running the removal again resumes the work.
    """
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_path / "extraction"
    if not extraction_directory.is_dir():
        message = (
            f"\n\nNo extraction directory was found at '{extraction_directory}'.\n"
            "Check that the cache directory is the one that extraction writes to.\n\n"
        )
        raise FileNotFoundError(message)

    records_directory = cache_path / "records"
    unfinished_record_names = _find_unfinished_records(records_directory) if records_directory.is_dir() else []
    if unfinished_record_names:
        message = (
            f"\n\nThe extraction records {', '.join(unfinished_record_names)} in '{records_directory}' show a log "
            "whose extraction started but has not finished.\n"
            "Either an extraction is running, in which case wait for it to end, or one was interrupted, in which "
            "case call `s3logextraction reset extraction` first. The cache is left untouched.\n\n"
        )
        raise RuntimeError(message)

    excluded_ip_set = frozenset(excluded_ips) if excluded_ips is not None else get_excluded_ips()
    if not excluded_ip_set:
        return {}

    max_workers = _handle_max_workers(workers=workers)
    batches = _iter_batches(_iter_object_key_directories(extraction_directory))
    remove_batch = functools.partial(
        _remove_batch,
        extraction_directory=extraction_directory,
        excluded_ips=excluded_ip_set,
        use_encryption=use_encryption,
    )

    records: dict[str, dict[str, int]] = {}
    for batch_records in _map_batches(batch_function=remove_batch, batches=batches, max_workers=max_workers):
        records.update(batch_records)

    # Pruned only once every worker is done, so that no worker finds a directory removed from under it
    # A directory may already be gone as the emptied parent of another
    for object_key in records:
        object_key_directory = extraction_directory / object_key
        if object_key_directory.is_dir() and not any(
            (object_key_directory / file_name).exists() for file_name in _PER_REQUEST_FILE_NAMES
        ):
            _prune_empty_directories(directory=object_key_directory, extraction_directory=extraction_directory)

    sorted_records = dict(sorted(records.items()))
    return sorted_records

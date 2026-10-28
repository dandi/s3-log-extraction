import collections
import collections.abc
import concurrent.futures
import dataclasses
import functools
import os
import pathlib

from ._ip_utils import _read_ips_from_file
from ..config import get_cache_directory, get_excluded_ips
from ..utils.parallel import _handle_max_workers

# The per-request files that extraction writes together into every object key directory, line-aligned
_PER_REQUEST_FILE_NAMES = ("timestamps.txt", "bytes_sent.txt", "ips.txt", "download.txt")

# Object key directories are handed to each worker in batches of this size, to keep the cost of submitting work small
# next to the cost of scanning it
_BATCH_SIZE = 1_000


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExcludedIpScan:
    """
    The result of scanning the extraction cache for records made by an excluded IP address.

    Attributes
    ----------
    records : dict of str to dict of str to int
        For each object key with any record of an excluded address, the number of its records made by each such
        address. Object keys are paths relative to the ``extraction`` directory, and are sorted.
        An empty dictionary means the scanned object keys hold no record of an excluded address.
    object_key_count : int
        The number of object keys scanned, whether or not they hold a record of an excluded address.
        Zero when no excluded address was given, since the cache is then not scanned.
    """

    records: dict[str, dict[str, int]]
    object_key_count: int


def _raise_walk_error(error: OSError, /) -> None:
    """Raise an error of the walk over the extraction cache, which ``os.walk`` would otherwise ignore."""
    raise error


def _iter_object_key_directories(extraction_directory: pathlib.Path, /) -> collections.abc.Iterator[pathlib.Path]:
    """
    Yield every directory of the extraction cache that holds extracted requests, one at a time.

    An object key directory is any directory holding at least one of the per-request files. The tree is walked
    lazily, so memory does not grow with the number of object keys. A directory that cannot be listed, such as one
    the current user may not read, raises its error rather than being skipped.
    """
    for directory, _, file_names in os.walk(top=extraction_directory, onerror=_raise_walk_error):
        if any(file_name in _PER_REQUEST_FILE_NAMES for file_name in file_names):
            yield pathlib.Path(directory)


def _count_lines(file_path: pathlib.Path, /) -> int:
    """Count the non-empty lines of a plaintext per-request file without holding its lines in memory."""
    with file_path.open(mode="rb") as file_stream:
        line_count = sum(1 for line in file_stream if line.strip())
    return line_count


def _scan_object_key_directory(
    *,
    object_key_directory: pathlib.Path,
    excluded_ips: frozenset[str],
    use_encryption: bool,
) -> dict[str, int]:
    """
    Count the records of one object key directory that were made by each excluded IP.

    The files of a directory with any such record are also checked for being line-aligned, since removing those
    records relies on line N of every file describing the same request.

    Raises
    ------
    RuntimeError
        If a per-request file is missing, or if a directory holding records of an excluded IP is not line-aligned.
    """
    missing_file_names = [
        file_name for file_name in _PER_REQUEST_FILE_NAMES if not (object_key_directory / file_name).exists()
    ]
    if missing_file_names:
        message = (
            f"\n\nThe extracted files for '{object_key_directory}' are incomplete: "
            f"{', '.join(missing_file_names)} not found.\n"
            "Extraction writes every per-request file of an object key together, so none of them should be absent. "
            "The extraction cache is either incompatible, if it was extracted before one of these files was "
            "introduced, or corrupted.\n\n"
        )
        raise RuntimeError(message)

    ips = _read_ips_from_file(file_path=object_key_directory / "ips.txt", use_encryption=use_encryption)
    record_counts_per_ip = dict(collections.Counter(ip for ip in ips if ip in excluded_ips))
    if not record_counts_per_ip:
        return record_counts_per_ip

    line_counts = {
        file_name: _count_lines(object_key_directory / file_name)
        for file_name in _PER_REQUEST_FILE_NAMES
        if file_name != "ips.txt"
    }
    line_counts["ips.txt"] = len(ips)
    if len(set(line_counts.values())) != 1:
        counts_description = ", ".join(f"{file_name}: {count}" for file_name, count in line_counts.items())
        message = (
            f"\n\nThe extracted files for '{object_key_directory}' are not line-aligned ({counts_description}).\n"
            "Line N of each file must describe the same request. The extraction cache is corrupted, most often by an "
            "extraction that was interrupted partway through writing these files.\n\n"
        )
        raise RuntimeError(message)

    return record_counts_per_ip


def _scan_batch(
    *,
    object_key_directories: list[pathlib.Path],
    extraction_directory: pathlib.Path,
    excluded_ips: frozenset[str],
    use_encryption: bool,
) -> ExcludedIpScan:
    """Scan a batch of object key directories, keeping the records of only those with a record of an excluded IP."""
    batch_records: dict[str, dict[str, int]] = {}
    for object_key_directory in object_key_directories:
        record_counts_per_ip = _scan_object_key_directory(
            object_key_directory=object_key_directory, excluded_ips=excluded_ips, use_encryption=use_encryption
        )
        if record_counts_per_ip:
            object_key = object_key_directory.relative_to(extraction_directory).as_posix()
            batch_records[object_key] = record_counts_per_ip

    batch_scan = ExcludedIpScan(records=batch_records, object_key_count=len(object_key_directories))
    return batch_scan


def _iter_batches(
    object_key_directories: collections.abc.Iterator[pathlib.Path], /
) -> collections.abc.Iterator[list[pathlib.Path]]:
    """Group a stream of object key directories into lists of at most ``_BATCH_SIZE``."""
    batch: list[pathlib.Path] = []
    for object_key_directory in object_key_directories:
        batch.append(object_key_directory)
        if len(batch) == _BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _map_batches[T](
    *,
    batch_function: collections.abc.Callable[..., T],
    batches: collections.abc.Iterator[list[pathlib.Path]],
    max_workers: int,
) -> collections.abc.Iterator[T]:
    """
    Apply a function to every batch of object key directories, yielding its results in order of completion.

    A ``max_workers`` of 1 runs in the current process. Otherwise only a few batches per worker are submitted ahead,
    so the walk is never drained into memory at once.
    """
    if max_workers == 1:
        for batch in batches:
            yield batch_function(object_key_directories=batch)
        return

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        pending_futures: set[concurrent.futures.Future[T]] = set()
        for batch in batches:
            pending_futures.add(executor.submit(batch_function, object_key_directories=batch))
            if len(pending_futures) >= 2 * max_workers:
                done_futures, pending_futures = concurrent.futures.wait(
                    pending_futures, return_when=concurrent.futures.FIRST_COMPLETED
                )
                for future in done_futures:
                    yield future.result()
        for future in concurrent.futures.as_completed(pending_futures):
            yield future.result()


def find_excluded_ip_records(
    *,
    cache_directory: str | pathlib.Path | None = None,
    excluded_ips: collections.abc.Iterable[str] | None = None,
    use_encryption: bool = True,
    workers: int = -2,
) -> ExcludedIpScan:
    """
    Find the records of the extraction cache that were made by an excluded IP address.

    Extraction drops the requests of excluded addresses, but records extracted before an address was excluded stay
    in the cache. This scans every object key of the cache for them. The cache is streamed one object key at a time,
    so memory stays flat however many object keys it holds.

    Parameters
    ----------
    cache_directory : path-like, optional
        The cache directory holding the ``extraction`` directory to scan.
        If not provided, the configured cache directory is used.
    excluded_ips : iterable of str, optional
        The addresses to look for, in canonical text form.
        If not provided, the result of ``s3_log_extraction.config.get_excluded_ips()`` is used.
    use_encryption : bool, default: True
        Whether the ``ips.txt`` files of the cache are encrypted.
        Pass ``False`` for a cache written without encryption.
    workers : int, default: -2
        The maximum number of worker processes.
        Negative values count back from all available cores, so -1 means all of them and -2 all but one.
        A value of 1 scans in the current process.

    Returns
    -------
    ExcludedIpScan
        The records of excluded addresses found in each object key, and the number of object keys scanned.

    Raises
    ------
    FileNotFoundError
        If the cache directory holds no ``extraction`` directory.
    OSError
        If a directory of the cache cannot be listed, such as one the current user may not read.
    RuntimeError
        If an object key directory is missing a per-request file, or if one holding a record of an excluded address
        is not line-aligned.
    """
    cache_path = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_path / "extraction"
    if not extraction_directory.is_dir():
        message = (
            f"\n\nNo extraction directory was found at '{extraction_directory}'.\n"
            "Check that the cache directory is the one that extraction writes to.\n\n"
        )
        raise FileNotFoundError(message)

    excluded_ip_set = frozenset(excluded_ips) if excluded_ips is not None else get_excluded_ips()
    if not excluded_ip_set:
        empty_scan = ExcludedIpScan(records={}, object_key_count=0)
        return empty_scan

    max_workers = _handle_max_workers(workers=workers)
    batches = _iter_batches(_iter_object_key_directories(extraction_directory))
    scan_batch = functools.partial(
        _scan_batch,
        extraction_directory=extraction_directory,
        excluded_ips=excluded_ip_set,
        use_encryption=use_encryption,
    )

    records: dict[str, dict[str, int]] = {}
    object_key_count = 0
    for batch_scan in _map_batches(batch_function=scan_batch, batches=batches, max_workers=max_workers):
        records.update(batch_scan.records)
        object_key_count += batch_scan.object_key_count

    scan = ExcludedIpScan(records=dict(sorted(records.items())), object_key_count=object_key_count)
    return scan

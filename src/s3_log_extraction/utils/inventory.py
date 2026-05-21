import collections
import csv
import gzip
import json
import pathlib
import typing


class LogBucketStats(typing.TypedDict):
    """Statistics for all objects in a local S3 Inventory.

    Attributes
    ----------
    file_count : int
        Total number of object keys recorded in the inventory.
    total_size_bytes : int or None
        Sum of object sizes in bytes, or ``None`` if the inventory does not
        include a ``Size`` column.
    """

    file_count: int
    total_size_bytes: int | None


class ExtractionCompletionStats(typing.TypedDict):
    """Completion statistics comparing processed records against inventory size.

    Attributes
    ----------
    processed_file_count : int
        Number of unique log filenames found in the remote extraction end record.
    inventory_file_count : int
        Total number of object keys recorded in the latest inventory snapshot.
    total_size_bytes : int or None
        Sum of object sizes in bytes, or ``None`` if the inventory lacks a
        ``Size`` column.
    percent_complete : float
        ``processed_file_count / inventory_file_count * 100`` (or ``0.0`` when
        ``inventory_file_count`` is zero).
    """

    processed_file_count: int
    inventory_file_count: int
    total_size_bytes: int | None
    percent_complete: float


def _extract_date_from_log_filename(filename: str) -> str | None:
    """
    Extract ``YYYY-MM-DD`` from a standard S3 server access log filename.

    S3 access log files are named ``YYYY-MM-DD-HH-MM-SS-UniqueString``.
    This function validates that the first three dash-separated components
    look like a valid calendar date.

    Parameters
    ----------
    filename : str
        The file name component of an S3 object key (no directory separators).

    Returns
    -------
    str or None
        The date string ``"YYYY-MM-DD"`` when the filename matches the S3
        access log naming convention, otherwise ``None``.
    """
    parts = filename.split("-")
    if len(parts) < 3:
        return None
    year_str, month_str, day_str = parts[0], parts[1], parts[2]
    if len(year_str) != 4 or len(month_str) != 2 or len(day_str) != 2:
        return None
    if not (year_str.isdigit() and month_str.isdigit() and day_str.isdigit()):
        return None
    return f"{year_str}-{month_str}-{day_str}"


def _load_inventory_manifest(
    inventory_directory: pathlib.Path,
) -> tuple[str, list[str], pathlib.Path]:
    """
    Load the most recent inventory manifest and return parsing metadata.

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.

    Returns
    -------
    source_bucket : str
        The bucket name recorded in ``manifest.json``.
    file_schema : list[str]
        Ordered list of column names from the inventory CSV.
    symlink_path : pathlib.Path
        Path to the ``symlink.txt`` file in the most recent hive partition.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    """
    hive_directory = inventory_directory / "hive"
    hive_partitions = sorted(hive_directory.glob("dt=*"))
    if not hive_partitions:
        message = f"No hive partitions found in {hive_directory}."
        raise FileNotFoundError(message)
    latest_partition = hive_partitions[-1]

    dt_value = latest_partition.name[len("dt=") :]
    date_part = dt_value[:10]
    time_part = dt_value[11:]
    timestamp_dir_name = f"{date_part}T{time_part}Z"
    manifest_path = inventory_directory / timestamp_dir_name / "manifest.json"

    with manifest_path.open(mode="r") as file_stream:
        manifest = json.load(fp=file_stream)

    source_bucket: str = manifest["sourceBucket"]
    file_schema = [col.strip() for col in manifest["fileSchema"].split(",")]
    symlink_path = latest_partition / "symlink.txt"

    return source_bucket, file_schema, symlink_path


def _read_s3_urls_from_local_inventory(
    inventory_directory: pathlib.Path,
    s3_root: str,
) -> dict[str, list[str]]:
    """
    Parse a local AWS S3 Inventory directory and return S3 URLs grouped by date.

    Reads the most recent hive partition, resolves the corresponding
    ``manifest.json``, follows the ``symlink.txt`` references, and parses
    every referenced ``data/*.csv.gz`` file.  Only keys whose full
    ``s3://`` URL starts with ``s3_root`` (trailing slash normalised) are
    included.

    Dates are extracted from each matching key using two strategies, applied
    in order:

    1. **Path-based** — if the path relative to ``s3_root`` starts with
       components that look like ``YYYY/MM/DD/…`` (each part is the expected
       number of digits), the date is taken from those three components.
    2. **Filename-based** — if path-based extraction fails (e.g. flat log
       files stored directly in the bucket root, or logs nested under an
       ``account-id/region/bucket/`` prefix before the date directories),
       the date is extracted from the log filename itself.  S3 server access
       log files use the naming convention
       ``YYYY-MM-DD-HH-MM-SS-UniqueString``, so the date is always present
       in the filename regardless of path depth.

    This dual strategy means the function correctly handles buckets that have
    a mix of flat-storage (legacy) and nested-storage (current) log files,
    even when ``s3_root`` is set to the outer bucket root.

    The AWS S3 Inventory directory must follow the standard layout::

        <inventory_directory>/
        ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
        │   └── manifest.json
        ├── data/
        │   └── <uuid>.csv.gz
        └── hive/
            └── dt=<YYYY-MM-DD-HH-MM>/
                └── symlink.txt

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.
    s3_root : str
        S3 prefix used to filter object keys
        (e.g. ``"s3://my-logs-bucket/logs"`` or ``"s3://my-logs-bucket"``
        for a bucket-root prefix that covers both flat and nested files).

    Returns
    -------
    dict[str, list[str]]
        Mapping of ``"YYYY-MM-DD"`` date strings to lists of matching S3
        URLs found in the inventory snapshot.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    ValueError
        If the ``Key`` column is absent from the inventory schema.
    """
    inventory_directory = pathlib.Path(inventory_directory)
    source_bucket, file_schema, symlink_path = _load_inventory_manifest(inventory_directory)

    if "Key" not in file_schema:
        message = f"'Key' column not found in inventory schema: {file_schema}"
        raise ValueError(message)
    key_index = file_schema.index("Key")

    # Read symlink.txt — each line is an S3 path to a data/*.csv.gz file.
    symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

    # Parse each local CSV.gz file referenced by the symlink.
    s3_root_prefix = s3_root.rstrip("/") + "/"
    inventory: dict[str, list[str]] = collections.defaultdict(list)
    for s3_data_path in symlink_lines:
        uuid_filename = s3_data_path.split("/")[-1]
        local_csv_gz_path = inventory_directory / "data" / uuid_filename
        with gzip.open(local_csv_gz_path, "rt", newline="") as gz_file:
            reader = csv.reader(gz_file)
            for row in reader:
                if len(row) <= key_index:
                    continue
                key = row[key_index]
                s3_url = f"s3://{source_bucket}/{key}"
                if not s3_url.startswith(s3_root_prefix):
                    continue
                relative_path = s3_url[len(s3_root_prefix) :]
                parts = relative_path.split("/")

                # Strategy 1: path-based date extraction for year/month/day/... structure.
                # Validate that the first three components look like a calendar date so that
                # deeply-nested paths (e.g. account-id/region/bucket/year/month/day/logfile)
                # are not misidentified.
                date = None
                if len(parts) >= 4:
                    year, month, day = parts[0], parts[1], parts[2]
                    if (
                        len(year) == 4
                        and year.isdigit()
                        and len(month) == 2
                        and month.isdigit()
                        and len(day) == 2
                        and day.isdigit()
                    ):
                        date = f"{year}-{month}-{day}"

                # Strategy 2: filename-based date extraction as a fallback.
                # Handles flat files stored directly in the bucket root as well as
                # files nested under a non-date prefix (e.g. account-id/region/bucket/).
                # S3 server access log filenames always start with YYYY-MM-DD-HH-MM-SS-*.
                if date is None:
                    date = _extract_date_from_log_filename(parts[-1])

                if date is None:
                    continue

                inventory[date].append(s3_url)

    return dict(inventory)


def get_log_bucket_stats(
    inventory_directory: pathlib.Path,
) -> LogBucketStats:
    """
    Return the file count and total size for all objects in the inventory.

    Reads the most recent hive partition of a local AWS S3 Inventory
    directory, follows the ``symlink.txt`` references, and accumulates
    statistics for every object key recorded in the inventory CSV files.

    The AWS S3 Inventory directory must follow the standard layout::

        <inventory_directory>/
        ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
        │   └── manifest.json
        ├── data/
        │   └── <uuid>.csv.gz
        └── hive/
            └── dt=<YYYY-MM-DD-HH-MM>/
                └── symlink.txt

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.

    Returns
    -------
    LogBucketStats
        A typed dict with:

        ``file_count`` : int
            Total number of object keys in the inventory.
        ``total_size_bytes`` : int or None
            Sum of object sizes in bytes, or ``None`` when the inventory
            does not include a ``Size`` column.

    Raises
    ------
    FileNotFoundError
        If no ``dt=*`` hive partitions are found.
    ValueError
        If the ``Key`` column is absent from the inventory schema.
    """
    inventory_directory = pathlib.Path(inventory_directory)
    _, file_schema, symlink_path = _load_inventory_manifest(inventory_directory)

    if "Key" not in file_schema:
        message = f"'Key' column not found in inventory schema: {file_schema}"
        raise ValueError(message)
    key_index = file_schema.index("Key")
    size_index = file_schema.index("Size") if "Size" in file_schema else None

    symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

    file_count = 0
    total_size_bytes: int | None = 0 if size_index is not None else None

    for s3_data_path in symlink_lines:
        uuid_filename = s3_data_path.split("/")[-1]
        local_csv_gz_path = inventory_directory / "data" / uuid_filename
        with gzip.open(local_csv_gz_path, "rt", newline="") as gz_file:
            reader = csv.reader(gz_file)
            for row in reader:
                if len(row) <= key_index:
                    continue
                file_count += 1
                if size_index is not None and len(row) > size_index:
                    total_size_bytes += int(row[size_index])  # type: ignore[operator]

    return LogBucketStats(file_count=file_count, total_size_bytes=total_size_bytes)


def get_extraction_completion(
    inventory_directory: pathlib.Path,
    *,
    cache_directory: pathlib.Path | None = None,
) -> ExtractionCompletionStats:
    """
    Compare remote extraction progress against the latest local inventory count.

    This helper reads:

    - latest inventory file count/size via :func:`get_log_bucket_stats`
    - current remote extraction end record
      (``RemoteS3LogAccessExtractor_s3-url-processing-end.txt``)

    and returns a simple percentage complete summary.

    Parameters
    ----------
    inventory_directory : pathlib.Path
        Root of the pre-downloaded S3 inventory tree.
    cache_directory : pathlib.Path or None, optional
        Cache directory containing the ``records/`` subdirectory.  If omitted,
        the configured default cache directory is used.

    Returns
    -------
    ExtractionCompletionStats
        A typed dict with processed count, inventory count, inventory total
        size (when available), and completion percentage.
    """
    from ..config import get_records_directory

    inventory_stats = get_log_bucket_stats(inventory_directory=inventory_directory)
    records_directory = get_records_directory(cache_directory=cache_directory)
    record_file_path = records_directory / "RemoteS3LogAccessExtractor_s3-url-processing-end.txt"

    processed_file_count = 0
    if record_file_path.exists():
        processed_file_count = len({line.strip() for line in record_file_path.read_text().splitlines() if line.strip()})

    inventory_file_count = inventory_stats["file_count"]
    percent_complete = 0.0 if inventory_file_count == 0 else processed_file_count / inventory_file_count * 100.0

    return ExtractionCompletionStats(
        processed_file_count=processed_file_count,
        inventory_file_count=inventory_file_count,
        total_size_bytes=inventory_stats["total_size_bytes"],
        percent_complete=percent_complete,
    )

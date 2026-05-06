import collections
import csv
import gzip
import json
import os
import pathlib
import warnings


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
        (e.g. ``"s3://my-logs-bucket/logs"``).

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

    # 1. Find the most recent hive partition (alphabetical sort works for dt=YYYY-MM-DD-HH-MM)
    hive_directory = inventory_directory / "hive"
    hive_partitions = sorted(hive_directory.glob("dt=*"))
    if not hive_partitions:
        message = f"No hive partitions found in {hive_directory}."
        raise FileNotFoundError(message)
    latest_partition = hive_partitions[-1]

    # 2. Derive the corresponding timestamp directory name.
    #    dt=2026-05-03-01-00  →  2026-05-03T01-00Z
    dt_value = latest_partition.name[len("dt=") :]  # e.g. "2026-05-03-01-00"
    date_part = dt_value[:10]  # "2026-05-03"
    time_part = dt_value[11:]  # "01-00"
    timestamp_dir_name = f"{date_part}T{time_part}Z"  # "2026-05-03T01-00Z"
    manifest_path = inventory_directory / timestamp_dir_name / "manifest.json"

    with manifest_path.open(mode="r") as file_stream:
        manifest = json.load(fp=file_stream)

    source_bucket: str = manifest["sourceBucket"]
    file_schema = [col.strip() for col in manifest["fileSchema"].split(",")]
    if "Key" not in file_schema:
        message = f"'Key' column not found in inventory schema: {file_schema}"
        raise ValueError(message)
    key_index = file_schema.index("Key")

    # 3. Read symlink.txt — each line is an S3 path to a data/*.csv.gz file.
    symlink_path = latest_partition / "symlink.txt"
    symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

    # 4. Parse each local CSV.gz file referenced by the symlink.
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
                if len(parts) < 4:
                    continue
                year, month, day = parts[0], parts[1], parts[2]
                date = f"{year}-{month}-{day}"
                inventory[date].append(s3_url)

    return dict(inventory)


def _handle_max_workers(*, workers: int) -> int:
    """
    Handle the number of workers for parallel processing.

    If workers is 0, it raises a warning and sets it to -2 (default).
    If workers is negative, it calculates the maximum number of workers based on CPU count.
    If workers is positive, it ensures it does not exceed the CPU count.

    Parameters
    ----------
    workers : int
        The requested number of worker processes.  Pass a negative value to
        use all but ``|workers| - 1`` CPUs; pass ``0`` to fall back to the
        default of ``-2``; pass a positive value to cap at the available CPU
        count.

    Returns
    -------
    int
        The effective number of worker processes to use.
    """
    if workers == 0:
        message = "The number of workers cannot be 0 - please set it to an integer. Falling back to default of -2."
        warnings.warn(message=message, stacklevel=2)
        workers = -2

    cpu_count = os.cpu_count()
    if workers < 0:
        max_workers = workers % cpu_count + 1
    elif workers > cpu_count:
        max_workers = cpu_count
    else:
        max_workers = workers

    return max_workers

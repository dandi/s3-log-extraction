"""Tests for the local-inventory-based URL discovery in RemoteS3LogAccessExtractor."""

import csv
import gzip
import io
import json
import pathlib
import unittest.mock

import pytest

from s3_log_extraction.extractors._remote_s3_log_access_extractor import RemoteS3LogAccessExtractor


def _make_extractor(tmp_path: pathlib.Path) -> RemoteS3LogAccessExtractor:
    """
    Return an extractor backed by *tmp_path* with empty processing state.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory used as the cache root.

    Returns
    -------
    RemoteS3LogAccessExtractor
        Extractor instance with ``processed_dates`` and
        ``s3_url_processing_end_record`` initialised to empty sets.
    """
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory)
    extractor.processed_dates = set()
    extractor.s3_url_processing_end_record = set()
    return extractor


def _build_inventory_directory(
    tmp_path: pathlib.Path,
    *,
    source_bucket: str,
    keys: list[str],
    timestamp: str = "2024-01-05T01-00Z",
    dt_partition: str = "dt=2024-01-05-01-00",
    file_schema: str = "Bucket, Key",
) -> pathlib.Path:
    """
    Create a minimal local AWS S3 Inventory directory structure.

    Writes a single ``data/*.csv.gz`` file whose rows are
    ``(source_bucket, key)`` for every *key* in *keys*.  The
    ``hive/<dt_partition>/symlink.txt`` references that file and the
    ``<timestamp>/manifest.json`` records the schema.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Root directory under which the inventory tree is created.
    source_bucket : str
        Value placed in the ``sourceBucket`` field of ``manifest.json``
        and written into each CSV row.
    keys : list[str]
        Object keys to include in the inventory CSV file.
    timestamp : str
        Name of the timestamped manifest directory, e.g. ``"2024-01-05T01-00Z"``.
    dt_partition : str
        Name of the hive partition directory, e.g. ``"dt=2024-01-05-01-00"``.
    file_schema : str
        Comma-separated column names written into ``manifest.json``.

    Returns
    -------
    pathlib.Path
        The root of the created inventory directory.
    """
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir(exist_ok=True)
    data_dir = inventory_dir / "data"
    data_dir.mkdir(exist_ok=True)
    hive_dir = inventory_dir / "hive"
    hive_dir.mkdir(exist_ok=True)

    # Write CSV.gz data file
    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gz:
        writer = csv.writer(gz)
        for key in keys:
            writer.writerow([source_bucket, key])
    uuid_filename = "test-uuid.csv.gz"
    (data_dir / uuid_filename).write_bytes(csv_buffer.getvalue())

    # Write manifest.json in the timestamp directory
    timestamp_dir = inventory_dir / timestamp
    timestamp_dir.mkdir(exist_ok=True)
    manifest = {
        "sourceBucket": source_bucket,
        "fileFormat": "CSV",
        "fileSchema": file_schema,
        "files": [
            {
                "key": f"inventory/{source_bucket}/data/{uuid_filename}",
                "size": len(csv_buffer.getvalue()),
            }
        ],
    }
    with (timestamp_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f)

    # Write hive partition and symlink.txt
    partition_dir = hive_dir / dt_partition
    partition_dir.mkdir(exist_ok=True)
    s3_data_ref = f"s3://inventory-bucket/inventory/{source_bucket}/data/{uuid_filename}"
    (partition_dir / "symlink.txt").write_text(s3_data_ref + "\n")

    return inventory_dir


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_local_inventory_basic(tmp_path: pathlib.Path) -> None:
    """
    All log-file keys from the inventory CSV are returned when no dates have
    been processed yet and all keys are under s3_root.
    """
    extractor = _make_extractor(tmp_path)
    s3_root = "s3://my-bucket/logs"
    source_bucket = "my-bucket"
    keys = [
        "logs/2024/01/01/2024-01-01-00-00-00-AAAA",
        "logs/2024/01/01/2024-01-01-00-05-00-BBBB",
        "logs/2024/01/02/2024-01-02-00-00-00-CCCC",
    ]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    result = extractor._get_unprocessed_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root=s3_root,
    )

    expected = {f"s3://{source_bucket}/{k}" for k in keys}
    assert set(result) == expected


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_local_inventory_skips_processed_dates(tmp_path: pathlib.Path) -> None:
    """
    Keys whose date is already in ``processed_dates`` are excluded from results.
    """
    extractor = _make_extractor(tmp_path)
    extractor.processed_dates = {"2024-01-01"}
    s3_root = "s3://my-bucket"
    source_bucket = "my-bucket"
    keys = [
        "2024/01/01/2024-01-01-00-00-00-AAAA",
        "2024/01/02/2024-01-02-00-00-00-BBBB",
    ]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    result = extractor._get_unprocessed_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root=s3_root,
    )

    assert result == ["s3://my-bucket/2024/01/02/2024-01-02-00-00-00-BBBB"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_local_inventory_skips_already_done_urls(tmp_path: pathlib.Path) -> None:
    """
    Individual URLs already in ``s3_url_processing_end_record`` are excluded
    even when their date is otherwise unprocessed.
    """
    s3_root = "s3://my-bucket"
    source_bucket = "my-bucket"
    already_done = "s3://my-bucket/2024/01/01/2024-01-01-00-00-00-AAAA"
    extractor = _make_extractor(tmp_path)
    extractor.s3_url_processing_end_record = {already_done}

    keys = [
        "2024/01/01/2024-01-01-00-00-00-AAAA",
        "2024/01/01/2024-01-01-00-05-00-BBBB",
    ]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    result = extractor._get_unprocessed_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root=s3_root,
    )

    assert result == ["s3://my-bucket/2024/01/01/2024-01-01-00-05-00-BBBB"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_local_inventory_ignores_non_matching_prefix(tmp_path: pathlib.Path) -> None:
    """
    Keys that do not start with the s3_root prefix are silently ignored.
    """
    extractor = _make_extractor(tmp_path)
    s3_root = "s3://my-bucket/logs"
    source_bucket = "my-bucket"
    # "other/..." does not start with "logs/" so it should be ignored
    keys = [
        "other/2024/01/01/2024-01-01-00-00-00-SKIP",
        "logs/2024/01/01/2024-01-01-00-00-00-KEEP",
    ]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    result = extractor._get_unprocessed_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root=s3_root,
    )

    assert result == ["s3://my-bucket/logs/2024/01/01/2024-01-01-00-00-00-KEEP"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_local_inventory_uses_latest_partition(tmp_path: pathlib.Path) -> None:
    """
    When multiple hive partitions exist, only the most recent one is used.
    """
    extractor = _make_extractor(tmp_path)
    s3_root = "s3://my-bucket"
    source_bucket = "my-bucket"

    # Build an older partition with one set of keys
    older_keys = ["2024/01/01/2024-01-01-00-00-00-OLD"]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        keys=older_keys,
        timestamp="2024-01-01T01-00Z",
        dt_partition="dt=2024-01-01-01-00",
    )

    # Add a newer partition that references a different CSV.gz with newer keys
    newer_keys = ["2024/01/05/2024-01-05-00-00-00-NEW"]
    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gz:
        writer = csv.writer(gz)
        for key in newer_keys:
            writer.writerow([source_bucket, key])
    new_uuid = "newer-uuid.csv.gz"
    (inventory_dir / "data" / new_uuid).write_bytes(csv_buffer.getvalue())

    new_timestamp = "2024-01-05T01-00Z"
    new_ts_dir = inventory_dir / new_timestamp
    new_ts_dir.mkdir()
    newer_manifest = {
        "sourceBucket": source_bucket,
        "fileFormat": "CSV",
        "fileSchema": "Bucket, Key",
        "files": [{"key": f"inventory/{source_bucket}/data/{new_uuid}", "size": 1}],
    }
    with (new_ts_dir / "manifest.json").open("w") as f:
        json.dump(newer_manifest, f)

    new_partition_dir = inventory_dir / "hive" / "dt=2024-01-05-01-00"
    new_partition_dir.mkdir()
    (new_partition_dir / "symlink.txt").write_text(f"s3://inventory-bucket/inventory/{source_bucket}/data/{new_uuid}\n")

    result = extractor._get_unprocessed_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root=s3_root,
    )

    assert result == ["s3://my-bucket/2024/01/05/2024-01-05-00-00-00-NEW"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_raises_when_both_manifest_and_inventory_provided(
    tmp_path: pathlib.Path,
) -> None:
    """
    Providing both ``manifest_file_path`` and ``inventory_directory`` simultaneously
    must raise a ``ValueError`` because they are mutually exclusive sources.
    """
    extractor = _make_extractor(tmp_path)

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text("{}")
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir()

    with pytest.raises(ValueError, match="Only one of 'manifest_file_path' or 'inventory_directory'"):
        extractor._get_unprocessed_s3_urls(
            manifest_file_path=manifest_file,
            s3_root="s3://my-bucket",
            inventory_directory=inventory_dir,
        )


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_remote_emits_performance_warning(tmp_path: pathlib.Path) -> None:
    """
    Calling ``_get_unprocessed_s3_urls_from_remote`` without an inventory directory
    must emit a ``UserWarning`` recommending S3 Inventory for better performance.
    """
    # _make_extractor already sets processed_dates and s3_url_processing_end_record to
    # empty sets; processed_years and processed_months_per_year default to empty in __init__.
    extractor = _make_extractor(tmp_path)

    # Patch _deploy_subprocess so no real S3 network calls are made.
    with unittest.mock.patch(
        "s3_log_extraction.extractors._remote_s3_log_access_extractor._deploy_subprocess",
        return_value="",
    ):
        with pytest.warns(UserWarning, match="Consider setting up AWS S3 Inventory"):
            extractor._get_unprocessed_s3_urls_from_remote(s3_root="s3://my-bucket")

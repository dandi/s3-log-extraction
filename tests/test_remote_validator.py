"""Tests for the remote-based validation methods on BaseValidator."""

import csv
import gzip
import io
import json
import pathlib
import unittest.mock

import pytest

from s3_log_extraction.validate import DownloadsLogicPreValidator


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

    Parameters
    ----------
    tmp_path : pathlib.Path
        Root directory under which the inventory tree is created.
    source_bucket : str
        Value placed in ``sourceBucket`` and in each CSV row.
    keys : list[str]
        Object keys to include in the inventory CSV.
    timestamp : str
        Name of the timestamped manifest directory.
    dt_partition : str
        Name of the hive partition directory.
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

    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gz:
        writer = csv.writer(gz)
        for key in keys:
            writer.writerow([source_bucket, key])
    uuid_filename = "test-uuid.csv.gz"
    (data_dir / uuid_filename).write_bytes(csv_buffer.getvalue())

    timestamp_dir = inventory_dir / timestamp
    timestamp_dir.mkdir(exist_ok=True)
    manifest = {
        "sourceBucket": source_bucket,
        "fileFormat": "CSV",
        "fileSchema": file_schema,
        "files": [{"key": f"inventory/{source_bucket}/data/{uuid_filename}", "size": 1}],
    }
    with (timestamp_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f)

    partition_dir = hive_dir / dt_partition
    partition_dir.mkdir(exist_ok=True)
    s3_data_ref = f"s3://inventory-bucket/inventory/{source_bucket}/data/{uuid_filename}"
    (partition_dir / "symlink.txt").write_text(s3_data_ref + "\n")

    return inventory_dir


def _make_validator(tmp_path: pathlib.Path) -> DownloadsLogicPreValidator:
    """
    Return a DownloadsLogicPreValidator whose records directory is inside *tmp_path*.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory used as the records root.

    Returns
    -------
    DownloadsLogicPreValidator
        Validator instance backed by an isolated records directory.
    """
    records_dir = tmp_path / "records"
    records_dir.mkdir()
    validator = DownloadsLogicPreValidator()
    # Override the records directory and record file path to use the temp directory
    validator.records_directory = records_dir
    validator.record_file_path = records_dir / "test_record.txt"
    validator.record = set()
    return validator


# ---------------------------------------------------------------------------
# Tests for _get_s3_urls_from_local_inventory
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_get_s3_urls_from_local_inventory_returns_all_matching_urls(tmp_path: pathlib.Path) -> None:
    """
    All keys that fall under s3_root should be returned as full S3 URLs.
    Keys outside the prefix must be excluded.
    """
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = [
        "logs/2024/01/01/file-A",
        "logs/2024/01/02/file-B",
        "other/2024/01/01/file-C",  # wrong prefix, must be excluded
    ]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    result = validator._get_s3_urls_from_local_inventory(
        inventory_directory=inventory_dir,
        s3_root="s3://my-bucket/logs",
    )

    assert set(result) == {
        "s3://my-bucket/logs/2024/01/01/file-A",
        "s3://my-bucket/logs/2024/01/02/file-B",
    }


@pytest.mark.ai_generated
def test_get_s3_urls_from_local_inventory_no_hive_partitions_raises(tmp_path: pathlib.Path) -> None:
    """FileNotFoundError is raised when the hive/ directory contains no dt=* partitions."""
    validator = _make_validator(tmp_path)
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir()
    hive_dir = inventory_dir / "hive"
    hive_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="No hive partitions found"):
        validator._get_s3_urls_from_local_inventory(
            inventory_directory=inventory_dir,
            s3_root="s3://my-bucket",
        )


@pytest.mark.ai_generated
def test_get_s3_urls_from_local_inventory_missing_key_column_raises(tmp_path: pathlib.Path) -> None:
    """ValueError is raised when the inventory schema has no 'Key' column."""
    validator = _make_validator(tmp_path)
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket="my-bucket",
        keys=["logs/2024/01/01/file-A"],
        file_schema="Bucket, Size",  # no Key column
    )

    with pytest.raises(ValueError, match="'Key' column not found"):
        validator._get_s3_urls_from_local_inventory(
            inventory_directory=inventory_dir,
            s3_root="s3://my-bucket",
        )


# ---------------------------------------------------------------------------
# Tests for validate_s3_bucket
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_validate_s3_bucket_raises_without_inventory_directory(tmp_path: pathlib.Path) -> None:
    """validate_s3_bucket raises NotImplementedError when inventory_directory is None."""
    validator = _make_validator(tmp_path)

    with pytest.raises(NotImplementedError, match="inventory_directory"):
        validator.validate_s3_bucket(s3_root="s3://my-bucket/logs")


@pytest.mark.ai_generated
def test_validate_s3_bucket_validates_unrecorded_urls(tmp_path: pathlib.Path) -> None:
    """
    validate_s3_bucket should assert file existence for files not yet in the record,
    then add the S3 URLs to the record.
    """
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = ["logs/2024/01/01/file-A", "logs/2024/01/01/file-B"]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    mock_fs = unittest.mock.MagicMock()
    mock_fs.exists.return_value = True
    with unittest.mock.patch("fsspec.filesystem", return_value=mock_fs):
        validator.validate_s3_bucket(s3_root=f"s3://{source_bucket}/logs", inventory_directory=inventory_dir)

    assert "s3://my-bucket/logs/2024/01/01/file-A" in validator.record
    assert "s3://my-bucket/logs/2024/01/01/file-B" in validator.record


@pytest.mark.ai_generated
def test_validate_s3_bucket_skips_already_recorded_urls(tmp_path: pathlib.Path) -> None:
    """URLs already present in the validator record are not re-checked."""
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = ["logs/2024/01/01/file-A", "logs/2024/01/01/file-B"]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    # Pre-populate the record with one URL
    already_done = "s3://my-bucket/logs/2024/01/01/file-A"
    validator.record.add(already_done)

    mock_fs = unittest.mock.MagicMock()
    mock_fs.exists.return_value = True
    with unittest.mock.patch("fsspec.filesystem", return_value=mock_fs):
        validator.validate_s3_bucket(s3_root=f"s3://{source_bucket}/logs", inventory_directory=inventory_dir)

    # Only the second file should have been checked
    assert mock_fs.exists.call_count == 1


@pytest.mark.ai_generated
def test_validate_s3_bucket_respects_limit(tmp_path: pathlib.Path) -> None:
    """Only up to `limit` files are checked when there are more unrecorded URLs."""
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = [f"logs/2024/01/01/file-{i}" for i in range(10)]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    mock_fs = unittest.mock.MagicMock()
    mock_fs.exists.return_value = True
    with unittest.mock.patch("fsspec.filesystem", return_value=mock_fs):
        validator.validate_s3_bucket(
            s3_root=f"s3://{source_bucket}/logs",
            inventory_directory=inventory_dir,
            limit=3,
        )

    assert len(validator.record) == 3


@pytest.mark.ai_generated
def test_validate_s3_bucket_persists_record_to_disk(tmp_path: pathlib.Path) -> None:
    """Successfully validated S3 URLs are written to the record file on disk."""
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = ["logs/2024/01/01/file-X"]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    mock_fs = unittest.mock.MagicMock()
    mock_fs.exists.return_value = True
    with unittest.mock.patch("fsspec.filesystem", return_value=mock_fs):
        validator.validate_s3_bucket(s3_root=f"s3://{source_bucket}/logs", inventory_directory=inventory_dir)

    saved = {line.strip() for line in validator.record_file_path.read_text().splitlines() if line.strip()}
    assert "s3://my-bucket/logs/2024/01/01/file-X" in saved


@pytest.mark.ai_generated
def test_validate_s3_bucket_skips_nonexistent_files(tmp_path: pathlib.Path) -> None:
    """URLs for which fsspec reports the file does not exist are not recorded."""
    validator = _make_validator(tmp_path)
    source_bucket = "my-bucket"
    keys = ["logs/2024/01/01/file-MISSING"]
    inventory_dir = _build_inventory_directory(tmp_path, source_bucket=source_bucket, keys=keys)

    mock_fs = unittest.mock.MagicMock()
    mock_fs.exists.return_value = False
    with unittest.mock.patch("fsspec.filesystem", return_value=mock_fs):
        validator.validate_s3_bucket(s3_root=f"s3://{source_bucket}/logs", inventory_directory=inventory_dir)

    assert "s3://my-bucket/logs/2024/01/01/file-MISSING" not in validator.record

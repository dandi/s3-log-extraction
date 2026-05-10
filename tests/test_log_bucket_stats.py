"""Mocked tests for the get_log_bucket_stats API helper and the 'stats' CLI command."""

import csv
import gzip
import io
import json
import pathlib

import pytest
from click.testing import CliRunner

from s3_log_extraction._command_line_interface._cli import _s3logextraction_cli
from s3_log_extraction.utils.inventory import get_log_bucket_stats


def _build_inventory_directory(
    tmp_path: pathlib.Path,
    *,
    source_bucket: str,
    rows: list[tuple],
    file_schema: str,
    timestamp: str = "2024-01-05T01-00Z",
    dt_partition: str = "dt=2024-01-05-01-00",
) -> pathlib.Path:
    """
    Create a minimal local AWS S3 Inventory directory for testing.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Root directory under which the inventory tree is created.
    source_bucket : str
        Value placed in the ``sourceBucket`` field of ``manifest.json``.
    rows : list[tuple]
        Rows to write into the CSV data file; each tuple must match *file_schema*.
    file_schema : str
        Comma-separated column names written into ``manifest.json``.
    timestamp : str
        Name of the timestamped manifest directory, e.g. ``"2024-01-05T01-00Z"``.
    dt_partition : str
        Name of the hive partition directory, e.g. ``"dt=2024-01-05-01-00"``.

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
        for row in rows:
            writer.writerow(row)
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


# ---------------------------------------------------------------------------
# API helper tests
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_get_log_bucket_stats_with_size_column(tmp_path: pathlib.Path) -> None:
    """
    File count and total size are correctly computed when a Size column is present.
    """
    source_bucket = "my-bucket"
    s3_root = "s3://my-bucket/logs"
    rows = [
        (source_bucket, "logs/2024-01-01-00-00-00-AAAA", 100),
        (source_bucket, "logs/2024-01-01-00-05-00-BBBB", 200),
        (source_bucket, "logs/2024-01-02-00-00-00-CCCC", 300),
        # This key is outside s3_root and should be ignored.
        (source_bucket, "other/2024-01-01-00-00-00-XXXX", 999),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root=s3_root)

    assert stats["file_count"] == 3
    assert stats["total_size_bytes"] == 600
    # The 'other/...' key (size 999) must not be counted since it is outside s3_root.


@pytest.mark.ai_generated
def test_get_log_bucket_stats_without_size_column(tmp_path: pathlib.Path) -> None:
    """
    File count is reported and total_size_bytes is None when Size is absent.
    """
    source_bucket = "my-bucket"
    s3_root = "s3://my-bucket/logs"
    rows = [
        (source_bucket, "logs/2024-01-01-00-00-00-AAAA"),
        (source_bucket, "logs/2024-01-02-00-00-00-BBBB"),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root=s3_root)

    assert stats["file_count"] == 2
    assert stats["total_size_bytes"] is None


@pytest.mark.ai_generated
def test_get_log_bucket_stats_empty_prefix(tmp_path: pathlib.Path) -> None:
    """
    No files match a prefix that doesn't exist in the inventory; count is 0.
    """
    source_bucket = "my-bucket"
    rows = [
        (source_bucket, "logs/2024-01-01-00-00-00-AAAA", 50),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root="s3://my-bucket/no-match")

    assert stats["file_count"] == 0
    assert stats["total_size_bytes"] == 0


@pytest.mark.ai_generated
def test_get_log_bucket_stats_returns_typed_dict(tmp_path: pathlib.Path) -> None:
    """
    The return value is a LogBucketStats typed dict with the expected keys.
    """
    source_bucket = "my-bucket"
    rows = [(source_bucket, "logs/2024-01-01-00-00-00-AAAA", 42)]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root="s3://my-bucket/logs")

    assert isinstance(stats, dict)
    assert "file_count" in stats
    assert "total_size_bytes" in stats


# ---------------------------------------------------------------------------
# CLI command tests
# ---------------------------------------------------------------------------


@pytest.mark.ai_generated
def test_stats_cli_with_size_column(tmp_path: pathlib.Path) -> None:
    """
    The 'stats' CLI command prints file count and total size when Size is present.
    """
    source_bucket = "my-bucket"
    s3_root = "s3://my-bucket/logs"
    rows = [
        (source_bucket, "logs/2024-01-01-00-00-00-AAAA", 100),
        (source_bucket, "logs/2024-01-02-00-00-00-BBBB", 400),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    runner = CliRunner()
    result = runner.invoke(
        _s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir), "--prefix", s3_root],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "2" in result.output
    assert "500" in result.output


@pytest.mark.ai_generated
def test_stats_cli_without_size_column(tmp_path: pathlib.Path) -> None:
    """
    The 'stats' CLI command reports N/A for total size when Size column is absent.
    """
    source_bucket = "my-bucket"
    s3_root = "s3://my-bucket/logs"
    rows = [
        (source_bucket, "logs/2024-01-01-00-00-00-AAAA"),
        (source_bucket, "logs/2024-01-02-00-00-00-BBBB"),
        (source_bucket, "logs/2024-01-03-00-00-00-CCCC"),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key",
    )

    runner = CliRunner()
    result = runner.invoke(
        _s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir), "--prefix", s3_root],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "3" in result.output
    assert "N/A" in result.output


@pytest.mark.ai_generated
def test_stats_cli_shows_file_count_label(tmp_path: pathlib.Path) -> None:
    """
    The 'stats' CLI output contains the 'File count' label.
    """
    source_bucket = "my-bucket"
    rows = [(source_bucket, "logs/2024-01-01-00-00-00-AAAA", 10)]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    runner = CliRunner()
    result = runner.invoke(
        _s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir), "--prefix", "s3://my-bucket/logs"],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "File count" in result.output
    assert "Total size" in result.output

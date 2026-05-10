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

SOURCE_BUCKET = "my-bucket"

_API_PARAMS = [
    pytest.param(
        "Bucket, Key, Size",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-CCCC", 300),
            # Key outside s3_root — must be excluded.
            (SOURCE_BUCKET, "other/2024-01-01-00-00-00-XXXX", 999),
        ],
        "s3://my-bucket/logs",
        3,
        600,
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-BBBB"),
        ],
        "s3://my-bucket/logs",
        2,
        None,
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "s3_root", "expected_count", "expected_size"),
    _API_PARAMS,
)
def test_get_log_bucket_stats_with_prefix(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    s3_root: str,
    expected_count: int,
    expected_size: int | None,
) -> None:
    """
    File count and total size are correctly computed for a filtered prefix.

    Covers the ``Size`` column present and absent cases.  Also validates that
    the return value is a ``LogBucketStats`` typed dict with the expected keys.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema=file_schema,
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root=s3_root)

    assert stats["file_count"] == expected_count
    assert stats["total_size_bytes"] == expected_size
    # Structural check: must be a dict with both expected keys.
    assert isinstance(stats, dict)
    assert "file_count" in stats
    assert "total_size_bytes" in stats


@pytest.mark.ai_generated
def test_get_log_bucket_stats_empty_prefix(tmp_path: pathlib.Path) -> None:
    """
    No files match a prefix that doesn't exist in the inventory; count is 0.
    """
    rows = [(SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 50)]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir, s3_root="s3://my-bucket/no-match")

    assert stats["file_count"] == 0
    assert stats["total_size_bytes"] == 0


@pytest.mark.ai_generated
def test_get_log_bucket_stats_no_prefix_defaults_to_whole_bucket(tmp_path: pathlib.Path) -> None:
    """
    When s3_root is omitted, all keys in the inventory are counted (whole-bucket default).
    The source bucket is derived from manifest.json so no explicit prefix is required.
    """
    rows = [
        (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
        (SOURCE_BUCKET, "other/2024-01-01-00-00-00-XXXX", 200),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir)

    assert stats["file_count"] == 2
    assert stats["total_size_bytes"] == 300


# ---------------------------------------------------------------------------
# CLI command tests
# ---------------------------------------------------------------------------

_CLI_PARAMS = [
    pytest.param(
        "Bucket, Key, Size",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-BBBB", 400),
        ],
        ["--prefix", "s3://my-bucket/logs"],
        ["File count", "Total size", "2", "500"],
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-BBBB"),
            (SOURCE_BUCKET, "logs/2024-01-03-00-00-00-CCCC"),
        ],
        ["--prefix", "s3://my-bucket/logs"],
        ["File count", "3", "N/A"],
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "extra_args", "expected_in_output"),
    _CLI_PARAMS,
)
def test_stats_cli(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    extra_args: list[str],
    expected_in_output: list[str],
) -> None:
    """
    The 'stats' CLI command produces the expected output for each schema variant.

    Covers the ``Size`` column present and absent cases, and validates that the
    ``File count`` and ``Total size`` labels always appear.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema=file_schema,
    )

    runner = CliRunner()
    result = runner.invoke(
        _s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir), *extra_args],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    for expected in expected_in_output:
        assert expected in result.output


@pytest.mark.ai_generated
def test_stats_cli_no_prefix_defaults_to_whole_bucket(tmp_path: pathlib.Path) -> None:
    """
    When --prefix is omitted the CLI reports stats for every key in the inventory.
    """
    rows = [
        (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 50),
        (SOURCE_BUCKET, "other/2024-01-01-00-00-00-XXXX", 50),
    ]
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema="Bucket, Key, Size",
    )

    runner = CliRunner()
    result = runner.invoke(
        _s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "2" in result.output
    assert "100" in result.output

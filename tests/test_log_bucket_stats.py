"""Mocked tests for the get_log_bucket_stats API helper and the 'stats' CLI command."""

import csv
import gzip
import io
import json
import pathlib

import pytest
from click.testing import CliRunner

import s3_log_extraction._command_line_interface._cli as cli_module
from s3_log_extraction._command_line_interface._cli import s3logextraction_cli
from s3_log_extraction.utils.inventory import get_extraction_completion, get_log_bucket_stats


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
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "other/2024-01-02-00-05-00-DDDD", 400),
        ],
        4,
        1000,
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-BBBB"),
        ],
        2,
        None,
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "expected_count", "expected_size"),
    _API_PARAMS,
)
def test_get_log_bucket_stats(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    expected_count: int,
    expected_size: int | None,
) -> None:
    """
    All inventory keys are counted regardless of their path prefix.

    Covers the ``Size`` column present and absent cases.  Also validates that
    the return value is a ``LogBucketStats`` typed dict with the expected keys.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=rows,
        file_schema=file_schema,
    )

    stats = get_log_bucket_stats(inventory_directory=inventory_dir)

    assert stats["file_count"] == expected_count
    assert stats["total_size_bytes"] == expected_size
    # Structural check: must be a dict with both expected keys.
    assert isinstance(stats, dict)
    assert "file_count" in stats
    assert "total_size_bytes" in stats


# ---------------------------------------------------------------------------
# CLI command tests
# ---------------------------------------------------------------------------

_CLI_PARAMS = [
    pytest.param(
        "Bucket, Key, Size",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "other/2024-01-02-00-00-00-BBBB", 400),
            (SOURCE_BUCKET, "other/2024-01-02-00-05-00-CCCC", 500),
        ],
        ["File count", "Total size", "3", "1000"],
        id="with_size_column",
    ),
    pytest.param(
        "Bucket, Key",
        [
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA"),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-BBBB"),
            (SOURCE_BUCKET, "other/2024-01-03-00-00-00-CCCC"),
        ],
        ["File count", "3", "N/A"],
        id="without_size_column",
    ),
]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_schema", "rows", "expected_in_output"),
    _CLI_PARAMS,
)
def test_stats_cli(
    tmp_path: pathlib.Path,
    file_schema: str,
    rows: list[tuple],
    expected_in_output: list[str],
) -> None:
    """
    The 'stats' CLI command counts all inventory keys and produces the expected output.

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
        s3logextraction_cli,
        ["stats", "--inventory", str(inventory_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    for expected in expected_in_output:
        assert expected in result.output


@pytest.mark.ai_generated
def test_get_extraction_completion(
    tmp_path: pathlib.Path,
) -> None:
    """
    Completion helper compares end-record count against inventory file count.

    Also verifies that duplicate record entries are de-duplicated.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=[
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "logs/2024-01-02-00-05-00-DDDD", 400),
        ],
        file_schema="Bucket, Key, Size",
    )

    cache_dir = tmp_path / "cache"
    records_dir = cache_dir / "records"
    records_dir.mkdir(parents=True)
    (records_dir / "CustomExtractor-processing-end.txt").write_text(
        "2024-01-01-00-00-00-AAAA\n2024-01-01-00-00-00-AAAA\n2024-01-01-00-05-00-BBBB\n"
    )
    (records_dir / "CustomExtractor-processing-start.txt").write_text("2024-01-02-00-00-00-CCCC\n")

    completion = get_extraction_completion(inventory_directory=inventory_dir, cache_directory=cache_dir)

    assert completion["processed_file_count"] == 2
    assert completion["inventory_file_count"] == 4
    assert completion["percent_complete"] == 50.0


@pytest.mark.ai_generated
def test_completion_cli(
    tmp_path: pathlib.Path,
) -> None:
    """
    The completion CLI reports processed count, inventory count, and % complete.
    """
    inventory_dir = _build_inventory_directory(
        tmp_path,
        source_bucket=SOURCE_BUCKET,
        rows=[
            (SOURCE_BUCKET, "logs/2024-01-01-00-00-00-AAAA", 100),
            (SOURCE_BUCKET, "logs/2024-01-01-00-05-00-BBBB", 200),
            (SOURCE_BUCKET, "logs/2024-01-02-00-00-00-CCCC", 300),
            (SOURCE_BUCKET, "logs/2024-01-02-00-05-00-DDDD", 400),
        ],
        file_schema="Bucket, Key, Size",
    )

    cache_dir = tmp_path / "cache"
    records_dir = cache_dir / "records"
    records_dir.mkdir(parents=True)
    (records_dir / "AnotherName-processing-end.txt").write_text("2024-01-01-00-00-00-AAAA\n2024-01-01-00-05-00-BBBB\n")

    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        ["completion", "--inventory", str(inventory_dir), "--cache", str(cache_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert "Processed files" in result.output
    assert "Inventory files" in result.output
    assert "Percent complete" in result.output
    assert "50.00%" in result.output
    assert "Total size (B)" not in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("command", "target_function_name"),
    [
        pytest.param(["stop"], "stop_extraction", id="stop"),
        pytest.param(["reset", "extraction"], "reset_extraction", id="reset_extraction"),
        pytest.param(["update", "ip", "indexes"], "index_ips", id="update_ip_indexes"),
        pytest.param(["update", "ip", "regions"], "update_index_to_region_codes", id="update_ip_regions"),
        pytest.param(["update", "ip", "coordinates"], "update_region_code_coordinates", id="update_ip_coordinates"),
        pytest.param(["update", "summaries"], "generate_summaries", id="update_summaries"),
        pytest.param(["update", "totals"], "generate_all_dataset_totals", id="update_totals"),
    ],
)
def test_cli_commands_forward_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, command: list[str], target_function_name: str
) -> None:
    """
    Commands backed by cache-aware APIs accept ``--cache`` and forward it correctly.
    """
    captured: dict[str, object] = {}

    def _stub(**kwargs: object) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(cli_module, target_function_name, _stub)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(s3logextraction_cli, [*command, "--cache", str(cache_dir)])

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir


@pytest.mark.ai_generated
def test_update_summaries_archive_forwards_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``update summaries --mode archive`` passes ``cache_directory`` directly to ``generate_archive_summaries``.
    """
    captured: dict[str, pathlib.Path] = {}

    def _stub_generate_archive_summaries(cache_directory: pathlib.Path | str | None = None) -> None:
        captured["cache_directory"] = pathlib.Path(cache_directory) if cache_directory is not None else None

    monkeypatch.setattr(cli_module, "generate_archive_summaries", _stub_generate_archive_summaries)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        ["update", "summaries", "--mode", "archive", "--cache", str(cache_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir


@pytest.mark.ai_generated
def test_update_totals_archive_forwards_cache_directory(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    ``update totals --mode archive`` passes ``cache_directory`` directly to ``generate_archive_totals``.
    """
    captured: dict[str, pathlib.Path] = {}

    def _stub_generate_archive_totals(cache_directory: pathlib.Path | str | None = None) -> None:
        captured["cache_directory"] = pathlib.Path(cache_directory) if cache_directory is not None else None

    monkeypatch.setattr(cli_module, "generate_archive_totals", _stub_generate_archive_totals)

    cache_dir = tmp_path / "custom-cache"
    runner = CliRunner()
    result = runner.invoke(
        s3logextraction_cli,
        ["update", "totals", "--mode", "archive", "--cache", str(cache_dir)],
    )

    assert result.exit_code == 0, f"CLI failed: {result.output}"
    assert captured["cache_directory"] == cache_dir

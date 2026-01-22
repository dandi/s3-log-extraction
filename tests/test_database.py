"""Tests for the database module."""

import pathlib
import shutil

import polars
import py
import yaml

import s3_log_extraction.database
from s3_log_extraction.config import set_cache_directory


def test_bundle_database(tmpdir: py.path.local) -> None:
    """
    Test that bundle_database correctly creates a hive-partitioned Parquet database.

    This test verifies that the database bundling function:
    - Creates the correct directory structure
    - Generates Parquet files with correct schema
    - Creates blob index mapping file
    - Produces expected data content
    """
    tmpdir = pathlib.Path(tmpdir)
    base_directory = pathlib.Path(__file__).parent

    # Set up test extraction directory in temp location
    test_extraction_source = base_directory / "expected_output" / "extraction_for_database_test"
    test_cache_directory = tmpdir / "cache"
    test_extraction_directory = test_cache_directory / "extraction"
    shutil.copytree(src=test_extraction_source, dst=test_extraction_directory)

    # Mock the cache directory for bundle_database
    set_cache_directory(cache_directory=test_cache_directory)

    # Run bundle_database
    s3_log_extraction.database.bundle_database()

    # Verify the output structure
    output_sharing_directory = test_cache_directory / "sharing"
    assert output_sharing_directory.exists()

    output_database_directory = output_sharing_directory / "extracted_activity.parquet"
    assert output_database_directory.exists()

    # Load expected output for comparison
    expected_sharing_directory = base_directory / "expected_output" / "sharing_for_database_test"
    expected_database_directory = expected_sharing_directory / "extracted_activity.parquet"

    # Verify blob index mapping exists
    output_blob_index_file = output_sharing_directory / "blob_index_to_id.yaml"
    expected_blob_index_file = expected_sharing_directory / "blob_index_to_id.yaml"
    assert output_blob_index_file.exists()

    # Load and compare blob index mappings
    with output_blob_index_file.open(mode="r") as f:
        output_blob_index_to_id = yaml.safe_load(f)
    with expected_blob_index_file.open(mode="r") as f:
        expected_blob_index_to_id = yaml.safe_load(f)

    # Verify the mapping has correct structure (exact values may differ based on iteration order)
    assert len(output_blob_index_to_id) == len(expected_blob_index_to_id)
    assert set(output_blob_index_to_id.values()) == set(expected_blob_index_to_id.values())

    # Verify Parquet files exist and have correct structure
    output_parquet_files = sorted(output_database_directory.rglob("*.parquet"))
    expected_parquet_files = sorted(expected_database_directory.rglob("*.parquet"))

    # Check that we have Parquet files for the expected partitions
    assert len(output_parquet_files) > 0, "No Parquet files were generated"

    # Get relative paths for comparison
    output_parquet_paths = {f.relative_to(output_database_directory) for f in output_parquet_files}
    expected_parquet_paths = {f.relative_to(expected_database_directory) for f in expected_parquet_files}
    assert output_parquet_paths == expected_parquet_paths

    # Verify schema and basic content of Parquet files
    expected_schema = {
        "asset_type": polars.String,
        "blob_head": polars.String,
        "timestamp": polars.Int64,
        "blob_index": polars.Int64,
        "bytes_sent": polars.Int64,
        "indexed_ip": polars.Int64,
    }

    for output_file, expected_file in zip(output_parquet_files, expected_parquet_files):
        output_df = polars.read_parquet(output_file)
        expected_df = polars.read_parquet(expected_file)

        # Verify schema
        assert output_df.schema == expected_schema, f"Schema mismatch in {output_file}"

        # Verify shape
        assert output_df.shape == expected_df.shape, f"Shape mismatch in {output_file}"

        # Verify content (sort to ensure consistent comparison)
        output_sorted = output_df.sort(by=["timestamp", "blob_index"])
        expected_sorted = expected_df.sort(by=["timestamp", "blob_index"])

        # Remap blob_index values for comparison since they depend on iteration order
        # We'll compare all other columns and verify blob_index is valid
        for col in ["asset_type", "blob_head", "timestamp", "bytes_sent", "indexed_ip"]:
            assert output_sorted[col].to_list() == expected_sorted[col].to_list(), (
                f"Column {col} mismatch in {output_file}"
            )

        # Verify blob_index values are within valid range
        assert output_df["blob_index"].min() >= 0
        assert output_df["blob_index"].max() < len(output_blob_index_to_id)

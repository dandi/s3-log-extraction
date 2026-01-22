# Database Module Test Data

This directory contains test data for the `bundle_database()` function in the database module.

## Structure

The `blobs/` subdirectory contains asset directories with extraction data:
- Each asset directory is named with a hash-based blob ID
- The blob IDs are derived from the original dataset/file paths for consistency

### Asset Data Files

Each asset directory contains:
- `timestamps.txt`: Unix timestamps of access events (one per line)
- `bytes_sent.txt`: Number of bytes sent for each access (one per line)
- `indexed_ips.txt`: Anonymized IP indexes for each access (one per line)

These files are the actual extraction output from processing S3 logs, reused from the existing test extraction data to avoid duplication.

## Parquet Database Output

The `bundle_database()` function processes these files and generates:
- A hive-partitioned Parquet database at `sharing/extracted_activity.parquet/`
- A blob index mapping file at `sharing/blob_index_to_id.yaml`

See `tests/expected_output/sharing/` for the expected output structure.

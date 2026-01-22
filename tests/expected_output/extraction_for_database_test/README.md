# Test Extraction Data for Database Module

This directory contains minimal test extraction data used to test the `bundle_database()` function.

## Structure

The directory mimics the expected structure from S3 log extraction:

- `blobs/`: Contains 3 blob assets with different starting characters (1, 2, a)
- `zarr/`: Contains 2 zarr assets with different starting characters (3, b)

Each asset directory contains three files:
- `timestamps.txt`: Unix timestamps of access events
- `bytes_sent.txt`: Number of bytes sent for each access
- `indexed_ips.txt`: Indexed IP addresses for each access

## Test Data

The test data represents minimal access logs:
- Blob assets: 2 access events each (6 total events)
- Zarr assets: 1 access event each (2 total events)

This small dataset is sufficient to test:
- Database partitioning by asset type (blobs vs zarr)
- Database partitioning by blob head (first character of asset ID)
- Proper schema generation
- Blob index mapping creation

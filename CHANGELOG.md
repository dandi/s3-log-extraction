# CHANGELOG

# Upcoming



# v1.9.2

Added `number_of_requesters` field to `totals.json` (per dandiset) and `archive_totals.json` (archive-wide).
This reports the number of unique requester IP addresses per dandiset and for the entire archive.
To protect privacy, the count is rounded to the nearest 10 and a `"<10"` sentinel is used for counts
below 10 (the minimum threshold).
The unique requester count is intentionally not coupled to region information and is not reported
at the per-asset level.

Added a new `DownloadsLogicPreValidator` that detects aberrant raw S3 log lines where `bytes_sent` is a valid
number and is less than the object size (`total_bytes`), yet the HTTP status code is exactly `200`.
A `200` status indicates a complete download, so `bytes_sent` should equal the object size; any deviation is
considered aberrant and causes the validator to raise a `RuntimeError`.
The new protocol is also exposed through the CLI as `s3logextraction validate downloads_logic <directory>`.

Added `number_of_downloads` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of complete downloads (HTTP status `200`) per grouping (date, asset, or region).
Also added `total_number_of_downloads` to `totals.json` and `archive_totals.json`.


# v1.4.0

## Features

Added Docker images built and published to GHCR (GitHub Container Registry).

- `ghcr.io/dandi/s3-log-extraction:latest-minimal` — minimal install from the latest PyPI release
- `ghcr.io/dandi/s3-log-extraction:latest` — full install (all extras) from the latest PyPI release
- `ghcr.io/dandi/s3-log-extraction:dev` — full install built from the `main` branch



# v1.3.9

## Improvements

Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries.
This counts the number of S3 log lines (requests) per grouping (date, asset, or region).
Also added `total_number_of_requests` to `totals.json` and `archive_totals.json`.



# v1.3.8

## Features

Added a new `download` field to the GAWK extraction step. The field is stored as plain text (`download.txt`) alongside the other extraction files. Its value is `1` when the raw log line has exactly a `200` HTTP status code (a complete download, not a partial request) and `0` otherwise.


# v1.3.7

## Features

Added `inventory_s3_path` parameter to `RemoteS3LogAccessExtractor.extract_s3_bucket` and a new `--inventory` CLI
option.  When an S3 inventory path is provided, unprocessed log files are discovered from the weekly inventory
snapshot instead of performing live ``s5cmd ls`` calls against the bucket.  The inventory file must be a plain-text
file stored in S3 containing one full S3 URL per line.


# v1.3.0

## Features

Added functionality and tests for generating generic summaries. A big thanks to @rwblair for contributing this!

## Improvements

Exposed the flag `--batch-limit` to `update_index_to_region_codes` and increased the timeout to the IP Info API.

Removed all DANDI-specific functionality. This has been split into the extension package https://github.com/dandi/dandi-s3-log-extraction.

Added a logo to the project.

Added tests for the CLI.



# v1.2.0

## Features

Support for child instances of DANDI has been added by way of an `--api-url` flag on the CLI for `s3logextraction update summaries` and an `api_url` parameter for the corresponding API methods.

## Improvements

Upgraded CLI to use `rich_click` for better formatting.

## Fixes

Moved some exposed imports to local levels to allow successful import of package under minimal installation conditions.



# v1.1.3

## Features

Added `s3_log_extraction.extractors.RemoteS3LogAccessExtractor` class for running extraction remotely rather than on local files.
 - Also includes the `s3_log_extraction.extractors.DandiRemoteS3LogAccessExtractor` class for DANDI-specific options.

Added parallelization option for UNIX systems, with all but one CPU requested by default.

Now tracks 'unassociated' access activity for DANDI summaries, which includes all extracted log data for blobs that do not match to any currently known Dandiset.

Added `s3_log_extraction.dataase.bundle_database` method for creating a hive-partitioned Parquet-based database of the extraction cache for easier sharing.

## Improvements

Added `bogon` labeling for IP addresses that are not routable on the public internet, such as private IPs and reserved ranges. This improves the update iteration of the `s3logextraction update ip regions`.

## Fixes

Fixed an issue related to duplication of access activity for assets that are duplicated (with multiple associated asset paths) within a Dandiset. Summary reports for DANDI prior to 7/27/2025 over count due to this issue.

Fixed the running of `s3logextraction update summaries --mode dandi` when running without `skip` or `pick` options.



# v1.0.0

First official release of the revamped s3-log-extraction tool.

Please see the README for usage instructions.

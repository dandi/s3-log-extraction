# CHANGELOG

## Upcoming

### 📝 Documentation

- Rewrote this changelog to use the AGENTS.md section layout. ([#246](https://github.com/dandi/s3-log-extraction/pull/246))


## v1.9.12

### 🚀 Enhancement

- Added `s3logextraction completion` and `get_extraction_completion` for inventory-based extraction progress, including support for any end-record filename that ends with `processing-end.txt`. Also removed byte-size reporting from completion API and CLI output. ([#231](https://github.com/dandi/s3-log-extraction/pull/231))

### 🔩 Dependency Updates

- Swapped all runtime type checking from `pydantic` to `beartype`. ([#228](https://github.com/dandi/s3-log-extraction/pull/228))


## v1.9.2

### 🚀 Enhancement

- Added `number_of_requesters` field to `totals.json` (per dandiset) and `archive_totals.json` (archive-wide). This reports the number of unique requester IP addresses per dandiset and for the entire archive. To protect privacy, the count is rounded to the nearest 10 and a `"<10"` sentinel is used for counts below 10. The unique requester count is intentionally not coupled to region information and is not reported at the per-asset level. ([#220](https://github.com/dandi/s3-log-extraction/pull/220))

- Added a new `DownloadsLogicPreValidator` that detects aberrant raw S3 log lines where `bytes_sent` is a valid number and is less than the object size (`total_bytes`), yet the HTTP status code is exactly `200`. A `200` status indicates a complete download, so `bytes_sent` should equal the object size. Any deviation is considered aberrant and causes the validator to raise a `RuntimeError`. The new protocol is also exposed through the CLI as `s3logextraction validate downloads_logic <directory>`. ([#203](https://github.com/dandi/s3-log-extraction/pull/203))

- Added `number_of_downloads` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries. This counts the number of complete downloads (HTTP status `200`) per grouping. Also added `total_number_of_downloads` to `totals.json` and `archive_totals.json`. ([#213](https://github.com/dandi/s3-log-extraction/pull/213))


## v1.4.0

### 🚀 Enhancement

- Added Docker images built and published to GHCR (GitHub Container Registry). Available tags are `ghcr.io/dandi/s3-log-extraction:latest-minimal` for the minimal install from the latest PyPI release, `ghcr.io/dandi/s3-log-extraction:latest` for the full install from the latest PyPI release, and `ghcr.io/dandi/s3-log-extraction:dev` for the full install built from the `main` branch. ([#202](https://github.com/dandi/s3-log-extraction/pull/202))


## v1.3.9

### 🚀 Enhancement

- Added `number_of_requests` column to `by_day.tsv`, `by_asset.tsv`, and `by_region.tsv` summaries. This counts the number of S3 log lines per grouping. Also added `total_number_of_requests` to `totals.json` and `archive_totals.json`. ([#201](https://github.com/dandi/s3-log-extraction/pull/201))


## v1.3.8

### 🚀 Enhancement

- Added a new `download` field to the GAWK extraction step. The field is stored as plain text (`download.txt`) alongside the other extraction files. Its value is `1` when the raw log line has exactly a `200` HTTP status code and `0` otherwise. ([#198](https://github.com/dandi/s3-log-extraction/pull/198))


## v1.3.7

### 🚀 Enhancement

- Added `inventory_s3_path` parameter to `RemoteS3LogAccessExtractor.extract_s3_bucket` and a new `--inventory` CLI option. When an S3 inventory path is provided, unprocessed log files are discovered from the weekly inventory snapshot instead of performing live ``s5cmd ls`` calls against the bucket. The inventory file must be a plain-text file stored in S3 containing one full S3 URL per line. ([#195](https://github.com/dandi/s3-log-extraction/pull/195))


## v1.3.0

### 🚀 Enhancement

- Added functionality and tests for generating generic summaries. A big thanks to @rwblair for contributing this. ([#103](https://github.com/dandi/s3-log-extraction/pull/103))

- Exposed the flag `--batch-limit` to `update_index_to_region_codes` and increased the timeout to the IP Info API. ([#150](https://github.com/dandi/s3-log-extraction/pull/150))

- Removed all DANDI-specific functionality. This has been split into the extension package https://github.com/dandi/dandi-s3-log-extraction. ([#154](https://github.com/dandi/s3-log-extraction/pull/154))

### 📝 Documentation

- Added a logo to the project. ([#158](https://github.com/dandi/s3-log-extraction/pull/158))

### 🏠 Internal

- Added tests for the CLI. ([#170](https://github.com/dandi/s3-log-extraction/pull/170))


## v1.2.0

### 🚀 Enhancement

- Support for child instances of DANDI has been added by way of an `--api-url` flag on the CLI for `s3logextraction update summaries` and an `api_url` parameter for the corresponding API methods. ([#143](https://github.com/dandi/s3-log-extraction/pull/143))

### 🐛 Bug Fix

- Moved some exposed imports to local levels to allow successful import of the package under minimal installation conditions. ([#143](https://github.com/dandi/s3-log-extraction/pull/143))

### 🔩 Dependency Updates

- Upgraded the CLI to use `rich_click` for better formatting. ([#143](https://github.com/dandi/s3-log-extraction/pull/143))


## v1.1.3

### 🚀 Enhancement

- Added `s3_log_extraction.extractors.RemoteS3LogAccessExtractor` for running extraction remotely rather than on local files. Also added `s3_log_extraction.extractors.DandiRemoteS3LogAccessExtractor` for DANDI-specific options. ([#106](https://github.com/dandi/s3-log-extraction/pull/106), [#108](https://github.com/dandi/s3-log-extraction/pull/108))

- Added a parallelization option for UNIX systems, with all but one CPU requested by default. ([#105](https://github.com/dandi/s3-log-extraction/pull/105))

- Now tracks `unassociated` access activity for DANDI summaries, which includes all extracted log data for blobs that do not match to any currently known Dandiset. ([#124](https://github.com/dandi/s3-log-extraction/pull/124))

- Added `s3_log_extraction.dataase.bundle_database` for creating a hive-partitioned Parquet-based database of the extraction cache for easier sharing. ([#126](https://github.com/dandi/s3-log-extraction/pull/126))

- Added `bogon` labeling for IP addresses that are not routable on the public internet, such as private IPs and reserved ranges. This improves the update iteration of `s3logextraction update ip regions`. ([#109](https://github.com/dandi/s3-log-extraction/pull/109))

### 🐛 Bug Fix

- Fixed an issue related to duplication of access activity for assets that are duplicated, with multiple associated asset paths, within a Dandiset. Summary reports for DANDI prior to 7/27/2025 overcount due to this issue. ([#120](https://github.com/dandi/s3-log-extraction/pull/120))

- Fixed `s3logextraction update summaries --mode dandi` when running without `skip` or `pick` options. ([#109](https://github.com/dandi/s3-log-extraction/pull/109))


## v1.0.0

### 🚀 Enhancement

- First official release of the revamped `s3-log-extraction` tool. ([#102](https://github.com/dandi/s3-log-extraction/pull/102))

### 📝 Documentation

- Please see the README for usage instructions. ([#89](https://github.com/dandi/s3-log-extraction/pull/89))

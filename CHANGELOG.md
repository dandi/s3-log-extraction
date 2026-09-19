# CHANGELOG

## Upcoming

### 🚀 Enhancement

- The base directory that extraction runs create their temporary directories inside is now configurable, with `s3logextraction config tmp set <directory>` or the `--tmp` option of `extract`. Runs otherwise use the system temporary directory, which on many systems is a small RAM-backed `/tmp` that a large extraction can exhaust. ([#304](https://github.com/dandi/s3-log-extraction/pull/304))

### 🐛 Bug Fix

- Parallel local extraction no longer leaves an empty temporary directory behind on every call. The directory was created and pointed at by `EXTRACTION_DIRECTORY`, but never written to, since each worker overrides that with its own process directory. It was also never removed, and it redirected any later serial run of the same extractor away from the cache. ([#304](https://github.com/dandi/s3-log-extraction/pull/304))

- A GeoLite2 refresh that MaxMind refuses because the account's daily download allowance is spent now returns the cached copy with a warning instead of failing, on the same terms as a stale copy that cannot be refreshed. With no cached copy to fall back on, the refusal is still raised. ([#303](https://github.com/dandi/s3-log-extraction/pull/303))

- The IP ranges of GitHub are now recognized by their shape, as any entry of the published meta document that parses as an IPv4 network, instead of by skipping a fixed list of non-range keys. This stops the "Skipping invalid CIDR entry" warning on entries GitHub has added over time, such as its PGP public key blocks. ([#300](https://github.com/dandi/s3-log-extraction/pull/300))

### 🏠 Internal

- The test suite now exercises every line of the package without remote resources. The remote extractor runs end to end over the example logs served as `file://` URLs, and the remaining untested paths of the extractor helpers, summaries, CLI, IP utilities, inventory walk and bucket validator are covered as well. ([#305](https://github.com/dandi/s3-log-extraction/pull/305))

- Code quality improvements throughout, from a functionality-preserving review of the whole package. Duplicated blocks in `summarize/`, `validate/`, `extractors/`, the CLI and the S3 inventory walk are single-sourced, dead code and unused manifest entries are removed, `ruff` now selects every rule set, and `mypy` is adopted. No observable behavior, public API or published output changes. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- The remote test workflow now actually reuses the GeoLite2 database between runs. Its cache step pointed at a directory the package never writes to, so every run downloaded afresh; it now caches `~/.cache/s3_log_extraction/geolite2` under a weekly key. The three remote tests share one database per session, and skip rather than fail when the allowance is spent. ([#303](https://github.com/dandi/s3-log-extraction/pull/303))



## v1.11.2

### ⚠️ Breaking

- Requesters are now geolocated during summary generation by the new `IpRegionResolver`, which checks published cloud service and VPN ranges, then routability, then the local GeoLite2 database. The `ip_to_region.yaml` cache is gone, along with the `update ip regions` and `update ip refresh` commands, their API functions, and the `batch_size`/`batch_limit` rationing. No requester location is written to disk anymore. `generate_summaries` gains a `region_resolver` argument, with `MappingRegionResolver` provided for tests.

- `update ip coordinates` now locates the region labels of the published `by_region.tsv` summaries instead of the removed cache, so it runs after `update summaries`. It no longer writes a separate `service_coordinates.yaml`, and `load_ip_cache` and `write_ip_cache` only accept `region_codes_to_coordinates` as the cache type.

- `get_ip_stats` (and `s3logextraction stats`) classify extracted IP addresses by resolving them the same way the summaries do, with an optional `region_resolver`, instead of reading the removed cache. The `classified_ip_count`, `percent_classified`, `missing`, and `undetermined` entries of `IpStats` are gone, and percentages are of the extracted total.

### 🚀 Enhancement

- Replaced the IPInfo API with the MaxMind GeoLite2-City database, downloaded into `[cache_directory]/geolite2/` and refreshed weekly using `MAXMIND_ACCOUNT_ID` and `MAXMIND_LICENSE_KEY`. Lookups are local, so there is no request quota. Region labels now pair ISO 3166-1 alpha-3 with ISO 3166-2, as in `USA/CA` rather than `US/California`. A new `s3logextraction update ip database` command downloads the database explicitly.

- Removed the OpenCage geocoder. `update ip coordinates` now looks each region label up in ISO 3166 tables bundled with the package, giving every country and every Natural Earth subdivision a representative coordinate, so the step needs no credentials or network access. Coordinates of existing regions shift slightly when regenerated. The tables are rebuilt with `tools/build_region_coordinates.py`.

- `number_of_unique_countries` in the totals now converts the alpha-2 prefix of AWS region names to alpha-3 before counting, so requesters in a country and AWS regions in the same country count as one country.

- Reworked privacy protection around the `by_region.tsv` summaries. Values are no longer censored or rounded; instead the file is published only when an update moves more than `region_disclosure_threshold` (default `5`) resolved regions at once, so its totals drift out of step with the other summaries between publications. `privacy_threshold_minimum` is replaced by `region_disclosure_threshold` on the two summary functions and removed from the totals functions. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a public `is_resolved_region` helper to `ip_utils`, alongside `is_cloud_service_or_vpn_label`, so region labels can be classified outside the summaries on the same terms. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a `--threshold` flag to `s3logextraction update summaries`, in both default and `archive` mode, setting the `region_disclosure_threshold`. Defaults to `5`. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- `totals.json` and `archive_totals.json` now read their activity totals from the by-day summaries rather than the by-region summaries, so they stay in step with `by_day.tsv` while a `by_region.tsv` is withheld. Only `number_of_unique_regions` and `number_of_unique_countries` still come from the by-region summary, and both report `0` while it is withheld. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a `number_of_views` column to the `by_asset.tsv`, `by_day.tsv`, and `by_region.tsv` summaries, with `total_number_of_views` in the totals. A view is a maximal run of streaming (HTTP 206) requests from one IP to one asset with no gap longer than `SESSION_TIMEOUT_IN_SECONDS`, counted on the day of its first request. Full downloads are never views. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

- Summary generation now raises a `RuntimeError` when an asset's `timestamps.txt`, `download.txt`, or `ips.txt` is missing or not line-aligned, instead of quietly counting that asset as having no views and no downloads. A missing or short `download.txt` means the asset predates that file and must be re-extracted; any other case means the extraction cache is corrupted. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

### 🏠 Internal

- Consolidated the module-level constants of the `summarize` submodule into a public `globals.py`, so the disclosure threshold and session timeout can be read from outside. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a third example log collection of repeated access from several documentation-range requesters, with a mocked `ip_to_region` cache. The integration tests now cover a published `by_region.tsv` and values accumulating over more than one request per asset, day, and region. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Sessionized each asset once per dataset summary and shared the result across the by-asset, by-day, and by-region tables, so `ips.txt` is decrypted no more often than before. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

- Added a `Version Check` CI workflow that fails pull requests which modify `src/` or `pyproject.toml` without bumping the package version. ([#292](https://github.com/dandi/s3-log-extraction/pull/292))

### 🐛 Bug Fix

- Fixed the archive requester count, which double-counted requesters. `generate_archive_summaries` overwrote the deduplicated archive `requester_count.tsv` with the sum of per-dataset counts, counting a requester once per dataset it accessed; it no longer writes that file. Published `archive_totals.json` values of `number_of_requesters` were overstated by the cross-dataset overlap and will drop when next regenerated. ([#295](https://github.com/dandi/s3-log-extraction/pull/295))

- Fixed the daily IP cache update workflows so an exhausted IPInfo or OpenCage quota no longer wastes hours on doomed requests or crashes the run. `update ip regions` halts at the first quota error and leaves unprocessed IPs uncached for retry, `update ip coordinates` saves partial progress and exits cleanly, and the daily remote tests skip rather than fail. ([#292](https://github.com/dandi/s3-log-extraction/pull/292))



## v1.10.8

### 🚀 Enhancement

- Excluded known cloud service and VPN IPs (GitHub, AWS, GCP, VPN) from the `number_of_requesters` count reported in per-dataset, archive, and total summaries. ([#291](https://github.com/dandi/s3-log-extraction/pull/291))
- Strengthened encryption key derivation. `S3_LOG_EXTRACTION_PASSWORD` now passes through PBKDF2-HMAC-SHA256 rather than a single SHA-256 pass, weak passwords are rejected before any encryption runs, and a public `validate_password_strength` helper was added. ([#283](https://github.com/dandi/s3-log-extraction/pull/283))
- Added the `s3logextraction stats` CLI command and the `get_log_bucket_stats` API helper for summarizing S3 inventory. ([#224](https://github.com/dandi/s3-log-extraction/pull/224))
- Extended `s3logextraction stats` and added a `get_ip_stats` API helper reporting IP classification statistics from the IP cache, binning every cached IP into one of seven categories with counts and percentages. The command also gains `--cache` and `--encryption` flags. ([#274](https://github.com/dandi/s3-log-extraction/pull/274))

### 🐛 Bug Fix

- Narrowed `is_cloud_service_or_vpn_label` to only match genuine cloud and VPN service labels. It previously also matched unresolved-location labels such as `"unknown"`, silently dropping real requesters whose IPs could not be geolocated from `number_of_requesters`. ([#291](https://github.com/dandi/s3-log-extraction/pull/291))
- Fixed the IPInfo quota-exceeded fallback so daily remote tests return `undetermined` instead of crashing on Python 3.14 when a warning is emitted. ([#273](https://github.com/dandi/s3-log-extraction/pull/273))


## v1.10.2

### 📝 Documentation

- Rewrote this changelog to use the AGENTS.md section layout. ([#246](https://github.com/dandi/s3-log-extraction/pull/246))

### 🚀 Enhancement

- Added `s3logextraction update ip refresh` CLI command and `refresh_ip_to_region_codes` API. Re-checks a partition of the existing `ip_to_region` cache against IPInfo each run, recording any changes in a log file under `[cache_directory]/logs/`. The partition size is `ceil(cache_size / 90)`, selected deterministically by today's date so that the entire cache is refreshed over a 90-day cycle.



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

# CHANGELOG

# Upcoming

## Features

Added `s3_log_extraction.extractors.RemoteS3LogAccessExtractor` class for running extraction remotely rather than on local files.
 - Also includes the `s3_log_extraction.extractors.DandiRemoteS3LogAccessExtractor` class for DANDI-specific options.

Added parallelization option for UNIX systems, with all but one CPU requested by default.

Now tracks 'unassociated' access activity for DANDI summaries, which includes all extracted log data for blobs that do not match to any currently known Dandiset.

Added `s3_log_extraction.dataase.bundle_database` method for creating a hive-partitioned Parquet-based database of the extraction cache for easier sharing.

# Improvements

Added `bogon` labeling for IP addresses that are not routable on the public internet, such as private IPs and reserved ranges. This improves the update iteration of the `s3logextraction update ip regions`.

## Fixes

Fixed an issue related to duplication of access activity for assets that are duplicated (with multiple associated asset paths) within a Dandiset. Summary reports for DANDI prior to 7/27/2025 over count due to this issue.

Fixed the running of `s3logextraction update summaries --mode dandi` when running without `skip` or `pick` options.



# v1.0.0

First official release of the revamped s3-log-extraction tool.

Please see the README for usage instructions.

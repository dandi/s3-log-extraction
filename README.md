<p align="center">
  <h1 align="center">S3 Log Extraction</h3>
  <p align="center">
    <a href="https://pypi.org/project/s3_log_extraction/"><img alt="Ubuntu" src="https://img.shields.io/badge/Ubuntu-E95420?style=flat&logo=ubuntu&logoColor=white"></a>
    <a href="https://pypi.org/project/s3_log_extraction/"><img alt="Supported Python versions" src="https://img.shields.io/pypi/pyversions/dandi_s3_log_parser.svg"></a>
    <a href="https://codecov.io/github/dandi/s3_log_extraction?branch=main"><img alt="codecov" src="https://codecov.io/github/dandi/s3_log_extraction/coverage.svg?branch=main"></a>
  </p>
  <p align="center">
    <a href="https://pypi.org/project/s3_log_extraction/"><img alt="PyPI latest release version" src="https://badge.fury.io/py/dandi_s3_log_parser.svg?id=py&kill_cache=1"></a>
    <a href="https://github.com/dandi/s3_log_extraction/blob/main/license.txt"><img alt="License: BSD-3" src="https://img.shields.io/pypi/l/dandi_s3_log_parser.svg"></a>
  </p>
  <p align="center">
    <a href="https://github.com/psf/black"><img alt="Python code style: Black" src="https://img.shields.io/badge/python_code_style-black-000000.svg"></a>
    <a href="https://github.com/astral-sh/ruff"><img alt="Python code style: Ruff" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
  </p>
</p>

Extraction of minimal information from consolidated raw S3 logs for public sharing and plotting.

Developed for the [DANDI Archive](https://dandiarchive.org/).

Read more about [S3 logging on AWS](https://web.archive.org/web/20240807191829/https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html).



## Installation

```bash
pip install s3-log-extraction
```



## Generic Usage

[Optional] Configure a non-default cache directory on a mounted disk that has sufficient space (the default is placed under `~/.cache`). This will be the main location where extracted logs and other useful information will be stored.

```bash
s3logextraction config cache set < new cache directory >
```

To extract the logs:

```bash
s3logextraction extract < log directory >
```

**NOTE**: If you feel like this command is taking a long time on your system, DO NOT interrupt it via `ctrl+C` or `pkill`. Instead, you can safely interrupt it by running:

```bash
s3logextraction stop
```

This will allow it to finish processing the current batch of logs and then exit gracefully.

After your logs are extracted, generate anonymized indexes for each IP address:

```bash
s3logextraction update ip indexes
````

Next, ensure some required environment variables related to external services are set:

1. **IPINFO_API_KEY**
  - Access token for the [ipinfo.io](ipinfo.io) service.
  - Extracts geographic region information in ISO 3166 format (e.g. "US/California") for anonymized statistics.
2. **OPENCAGE_API_KEY**
  - Access token for the [opencagedata.com](opencagedata.com) service.
  - Maps the ISO 3166 codes from the first step to latitude and longitude coordinates for the geographic heat maps used in visualizations.

```bash
export IPINFO_API_KEY="your_token_here"
export OPENCAGE_API_KEY="your_token_here"
```

To update the region codes and their coordinates:

```bash
s3logextraction update ip regions
s3logextraction update ip coordinates
```

To generate top-level summaries and totals (that is, per dataset):

```bash
s3logextraction update summaries
s3logextraction update totals
```

Finally, to generate archive-wide summaries and totals:

```bash
s3logextraction update summaries --mode archive
s3logextraction update totals --mode archive
```



## DANDI Usage

Usage on the DANDI archive logs requires a bit more customization. This is mostly achieved by way of the flag `--mode dandi` on various operations.

Begin by ensuring a special required environment variable is set:

1. **S3_LOG_EXTRACTION_PASSWORD**
  - Various sensitive information on Drogon is encrypted using this password, including...
  - ...the regular expression for all associated Drogon IPs.
  - ...the IP index and geolocation caches.

This allows us to store full IP information in a persistent way (in case we need to go back and do a lookup) while still being secure.

```bash
export S3_LOG_EXTRACTION_PASSWORD="ask_yarik_or_cody_for_password"
```

In fresh environments, the cache should be specified as:

```bash
s3_log_extraction set_cache /mnt/backup/dandi/s3-logs-extraction-cache
```

To run all the steps (such as for daily updates):

```bash
s3_log_extraction extract_logs /mnt/backup/dandi/dandiarchive-logs --mode dandi
s3_log_extraction index_ips
s3_log_extraction update_indexed_region_codes
s3_log_extraction update_region_code_coordinates
s3logextraction update summaries --mode dandi
s3logextraction update totals --mode dandi
s3logextraction update summaries --mode archive
s3logextraction update totals --mode archive
```



## Developer Notes

Throughout the codebase, various processes are referred to in the following ways:

- parallelized: The process can be run in parallel across multiple workers, which increases throughput.
- interruptible: The process can be safely interrupted (`ctrl+C` or `pkill`) with only a very low chance of causing corruption. For parallelized interruption you may have to either `pkill` the main dispatch process or spam `ctrl+C` multiple times.
- updatable: The process can be resumed from the last checkpoint without losing any progress. It can also be run fresh at different times, such as on a CRON cycle, and it will only interact with unprocessed data.

### Workflow

# add mermaid diagram of low-level workflow

```mermaid
flowchart TD
    A[Start] --> B[Extract logs]
    B --> C{Is extraction complete?}
    C -- Yes --> D[Update IP indexes]
    C -- No --> B
    D --> E{Are region codes updated?}
    E -- Yes --> F[Update coordinates]
    E -- No --> D
    F --> G[Generate summaries]
    G --> H[Generate totals]
    H --> I[End]
```


### Performance

By leveraging `GAWK`, this version of the S3 log handling is considerably more efficient than the previous attempts.

The previous attempt used a multistep process which took several days to run (even on multiple workers). It also required an additional ~200 GB cache to allow lazy updates of the per-object bins.

This version requires no intermediate cache, stores only the minimal amount of data to be shared, and takes less than a day to do a fresh run (and is also lazy regarding daily CRON updates).

### Validation

In lieu of attempting fully validated parsing of each and every line from the log files (which is a hard, unsolved problem - see [s3-log-parser](https://github.com/dandi/s3-log-parser)), we instead validate the heuristics in a targeted manner through specific validation scripts.

These can also be used to verify the current state of the extraction process, such as warning about corrupt records or incomplete cache files.

### Submission of line decoding errors

Should you discover any lines in your S3 log files that cause failures in the codebase, please email them to the core maintainer (cody.c.baker.phd@gmail.com) before raising issues or submitting PRs contributing them as examples, to more easily correct any aspects that might require anonymization.

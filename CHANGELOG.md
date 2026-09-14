# CHANGELOG

## Upcoming

### 🐛 Bug Fix

- A refresh of the GeoLite2 database that MaxMind refuses because the account's daily download allowance is spent no longer fails outright when a usable copy is already in the cache directory. The cached copy is returned with a warning instead, on the same terms as a stale copy that cannot be refreshed for want of credentials. The allowance is a property of the account rather than of this package and resets on its own, so a run that already has a database to geolocate with has no reason to stop. With no copy to fall back on the refusal is still raised, since there is then nothing to geolocate with. ([#303](https://github.com/dandi/s3-log-extraction/pull/303))

- The IP ranges of GitHub are now recognized by their shape, as any entry of the published meta document that parses as an IPv4 network, instead of by skipping a fixed list of non-range keys. GitHub adds listings to the document over time, most recently its PGP public key blocks, and each of those entries was previously handed to the resolver as a CIDR and reported with a "Skipping invalid CIDR entry" warning on every run. ([#300](https://github.com/dandi/s3-log-extraction/pull/300))

### 🏠 Internal

- Reshaped four pieces of summary generation, with no change to any published file. The by-day and by-region dataset summaries ran the same aggregation with one column renamed, and the archive by-day and by-region aggregations likewise; both pairs now share a helper. Two asymmetries between each pair are deliberately kept rather than tidied away: the by-day summaries sort their rows and the by-region ones publish in the order regions were first seen, so the sort stays with the caller that wants it; and the archive by-region aggregation skips an archive with nothing published while the by-day one raises, which is existing behavior and now carries a comment saying so. The by-asset summary no longer recovers the extraction directory by walking three parents up from the summary file, a derivation that only held while both trees shared a cache directory, and is handed the path instead; the `asset_path` column it computes against that path is unchanged. The inversion of a cloud service's published address ranges is now cached rather than rebuilt for every region located, over a listing that runs to several thousand entries for AWS. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Shared two blocks that the local and remote extractors held in common, with no change to behavior. Their `_run_extraction` bodies were identical in every character, as was the loop that copies each child process's output back into the extraction directory, down to all six progress-bar arguments; both now live in `extractors/_utils.py`. Both classes keep their own `_run_extraction` method and their own class surface, so nothing either exposes has moved, and the broader question of whether one should inherit from the other is untouched. The environment dictionary is still mutated in place rather than copied, so an overridden extraction directory persists on the extractor for later calls exactly as before. The two expressions that gather the files to copy are deliberately left where they are, because they are not the same: the remote extractor keeps only entries that are files and the local one does not filter. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Collapsed three repetitions in the command line interface, with no change to behavior or to help output. The coercion of an optional `--cache` value to a path was written out nine times, sometimes bound to a local and sometimes inlined, and is now one helper. Six of the nine `--cache` option blocks were byte-identical, about sixty lines expressing one idea, and now apply a shared decorator placed where each literal block stood; the three that genuinely differ, in help text or in requiring the directory to already exist, keep their own declarations. The five pre-validation protocol names appeared three times in one function, as the choice list, the annotation, and five near-identical `match` arms, and are now one mapping that drives both the choices and the dispatch. Because this is the one change that touches how options are *declared*, the help text of the root command and all nineteen commands and subcommands was captured before and after and is byte-identical, as is every parameter of every command compared attribute by attribute, declaration order included. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Single-sourced the S3 inventory walk in `utils/inventory.py`, with no change to behavior. Resolving a local inventory snapshot and walking its rows was written out twice, once in `_read_s3_urls_from_local_inventory` and once in `get_log_bucket_stats`, about fifteen lines each covering the manifest load, the `Key` column check and its message, the symlink listing, and the nested loop that opens every referenced `data/*.csv.gz` and skips rows too short to carry a key. Both now drive `_load_inventory_snapshot` and `_iter_inventory_rows`; the walk stays a generator, so these files, which run to millions of rows, are still never held in memory at once. The schema is resolved before any row is read, which keeps an inventory that declares a `Size` column but holds no rows reporting a total of zero bytes rather than none. Two date checks that validated the same three components in two different spellings now share `_format_date_if_valid`, with each caller keeping its own surrounding length check, and the region categorization of `get_ip_stats` moves out of the function body to where it can be read and tested on its own. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Collapsed the five pre-validators onto two shared helpers in `validate/`, with no change to behavior. Each of the five carried the same four statements to hash its AWK script, and a `_run_validation` that differed from its siblings only in the leading phrase of its error message and in one argument; they now delegate to `_hash_awk_script_file` and `_run_awk_validation`, and the one docstring that existed among each set survives on the helper. The five `__hash__` methods are kept rather than folded into `BaseValidator.__hash__`, which has a different definition and is public extension surface pinned by a test. The hash value names each validator's record file, so a change to it would orphan every existing record and silently re-validate every log; all five values were captured before the change and are identical after, down to the file names they produce. The one argument that varies is the subprocess environment, which the extraction heuristic passes as a single-key mapping that *replaces* the environment rather than adding to it, and which the helper therefore forwards unchanged rather than merging: the five validators were run before and after the change and hand the subprocess the same command, the same flags and the same environment, and raise byte-identical messages. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Collapsed four copy-pasted blocks in `summarize/` into three shared helpers, with no change to behavior. The four-line loop that backfills the activity columns a summary written before views were reported lacks, and coerces them to `int64`, appeared verbatim in four places across three modules; it is now `_coerce_activity_columns`. The seven-key totals dictionary that is the published schema of both `totals.json` and `archive_totals.json` was written out twice, which is precisely where a schema drift between the two would have hidden; it is now `_build_totals`. The requester-count parsing that accompanied it was duplicated too, and moves into that helper, which removes an `isinstance` check that was dead in the archive module (the value is always a string there) while keeping it live for the per-dataset module (where a dataset with no count file yields the integer zero). Six identical comprehensions reading one integer per line from an extraction file became `_read_integers_from_file`. Verified by the golden-file summary tests, and, because no test writes a sentinel count such as `"<50"` or omits a count file, by running both totals writers before and after the change across seven input shapes covering plain, sentinel, missing and mixed counts as well as a summary with no views column: all fourteen JSON outputs are byte-identical, sentinels still pass through as strings rather than being parsed as integers. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- The remote test workflow now actually reuses the GeoLite2 database between runs. Its cache step pointed at `~/.s3_log_extraction`, which is neither the cache directory the package writes the database to nor the configuration directory, so nothing was ever restored or saved and every run downloaded afresh. The step now caches `~/.cache/s3_log_extraction/geolite2` under a key that rotates weekly, matching how often a copy goes stale, since a cache entry is immutable once written and a fixed key would pin the first copy forever. Saving is separate from restoring and happens only when a database was actually obtained, so that a run which downloaded nothing cannot pin the week's key to an empty entry. The three remote tests each took their own temporary cache directory and so downloaded a copy apiece; they now share one resolved once per session. The scheduled run opts out of the cache so that it still downloads once a day and keeps checking that the credentials are accepted. When the allowance is spent and there is no cached copy to fall back on, the remote tests now report themselves skipped rather than failed, since an account-wide limit that resets on its own says nothing about the code under test. ([#303](https://github.com/dandi/s3-log-extraction/pull/303))

- Dropped the `hatch-vcs` build dependency and the `[tool.hatch.version]` block that named it. Hatchling only consults that block when `version` is listed in `[project].dynamic`, and this project states its version statically instead, so the block was never read and the dependency was never used for anything. Wheels built before and after the change are byte-identical, the same files with the same contents and the same metadata, both reporting the static version. The comment in `Dockerfile.dev` that gave this versioning as the reason for installing `git` is gone; the `git` install itself is untouched. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Reduced the `ruff` ignore list to the one entry that does anything. The linter selects `F`, `E` and `I`, which is Pyflakes, pycodestyle errors and isort, and ten of the eleven ignored rules belong to none of those three, so they could never be raised and never be suppressed. The list read as though the project enforced pathlib use, docstring conventions and bandit checks, when it enforced none of them. `F821` stays, and is genuinely load-bearing: without it the string annotations that name the lazily imported `geoip2` reader in four places are reported as undefined. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Corrected a set of annotations and names, with no change to behavior. `get_running_pids` is annotated `set[str]`, which is what its set comprehension has always returned, and `_request_cidr_range` is annotated `dict | list[str]`, since its VPN branch returns the split lines rather than parsed JSON. The `validate` command annotates its directory argument `str`, which is what rich_click passes it and what every other command in that file already declared. Summary generation takes the extraction timestamp format from the `TIMESTAMP_FORMAT` constant rather than repeating its literal, and drops a `str()` around a value that is already a string. The remote bucket scan drops an f-string that wrapped a single string expression. A `FileNotFoundError` message is bound to `message` rather than `msg`, and two JSON file handles are bound to `file_stream` rather than `io`, matching the rest of the package and no longer shadowing the name of a standard library module. `_collect_unique_ips` takes its arguments by keyword, and the five pre-validator `__init__` methods declare their `None` return, as the base class and `RemoteS3BucketValidator` already did. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Removed three pieces of unreachable code, with no change to behavior. The `_ip_in_cidr` helper of `ip_utils` had no callers left after the prefix-table matching of v1.11.2 replaced the per-address CIDR scan it performed, and it is gone along with the `warnings` import it alone needed. The remote extractor's scan of a bucket guarded three of its four `s5cmd ls` results against `None`, which `_deploy_subprocess` returns only when called with `ignore_errors=True`; none of those calls passes it, so the guards could never fire and are gone. Summary generation no longer renumbers the index of the by-day table before writing it, since that table is written with `index=False` and the renumbering could not reach the file. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Removed a batch of mechanical noise across twelve modules, with no change to behavior. Twenty-two locals that were assigned only to be returned on the next line now return their expression directly, four set comprehensions that only copied their iterable became `set()` calls, a `dict` comprehension with a constant value became `dict.fromkeys`, a `.keys()` was dropped from an iteration, and three `pass` statements were removed from command groups that already had a docstring. `reset_extraction` now deletes its records with a plain loop rather than draining a generator through a zero-length `collections.deque`. Every one of these is flagged by a `ruff` rule, so the change is verified by that rule set coming back clean. The validator record file names, which are derived from a hash of each validator's awk script or of its `_run_validation` bytecode, were confirmed byte-identical before and after, so existing validation caches remain valid. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

- Moved a pair of imports to where the project's own conventions put them, with no change to behavior. The two `from ..config import ...` statements inside functions of `utils/inventory.py` are now a single statement at the top of the module; the config package does not import `utils`, so there was no import cycle for the deferral to break, and importing the package and both functions still succeeds. Three test imports now name the public surface, `s3_log_extraction`, `s3_log_extraction.utils` and `s3_log_extraction.ip_utils`, rather than the private modules behind it, so that the tests exercise what callers actually import. The one test import that reaches into a module deliberately, to rebind a name that a command looks up at call time, is unchanged, as is the one that names a genuinely private helper. ([#302](https://github.com/dandi/s3-log-extraction/pull/302))

## v1.11.2

### ⚠️ Breaking

- Requesters are now geolocated while the summaries are generated, and the `ip_to_region.yaml` cache is gone along with the `s3logextraction update ip regions` and `update ip refresh` commands, the `update_ip_to_region_codes` and `refresh_ip_to_region_codes` functions, and the `batch_size`/`batch_limit` rationing that spread metered API calls over days. `update summaries` resolves every requester IP on the fly with the new `IpRegionResolver`: the published ranges of GitHub, AWS, GCP, and known VPN providers are checked first, indexed by prefix length so that the check is a handful of dictionary lookups per address rather than a scan of thousands of ranges, then whether the address is publicly routable, and only then the local GeoLite2 database. Results are memoized within a run, the database is opened only when an address needs it, and every run resolves against the current database, so there is nothing left to refresh on a cycle. No requester's location is written to disk anymore; only the aggregated `by_region.tsv` summaries are. `generate_summaries` gains a `region_resolver` argument, which defaults to an `IpRegionResolver` over the cache directory's database; a `MappingRegionResolver` over a fixed address-to-label mapping (with `missing` for absent addresses) is provided for tests and pipelines that resolve requesters by other means, and either satisfies the `RegionResolver` protocol. Running `update summaries` now needs network access for the service range listings, and the MaxMind credentials on the first run and whenever the database is more than a week old. The `missing` label no longer occurs in new summaries, since every address resolves to something; it is still recognized in summaries written earlier.

- `update ip coordinates` now locates the region labels of the published `by_region.tsv` summaries, per dataset and archive-wide, instead of the labels of the removed cache, so it runs after `update summaries`. It no longer raises when nothing has been summarized yet, and no longer writes a separate `service_coordinates.yaml`: `region_codes_to_coordinates.yaml` is the only file it maintains, and cloud service regions already in it are not looked up again. `load_ip_cache` and `write_ip_cache` only accept `region_codes_to_coordinates` as the cache type.

- `get_ip_stats` (and `s3logextraction stats`) classify the extracted IP addresses by resolving them the same way the summaries do, with an optional `region_resolver`, instead of reading the removed cache. The `classified_ip_count`, `percent_classified`, `missing`, and `undetermined` entries of `IpStats` are gone, since every extracted address now classifies and those labels are no longer produced; percentages are of the extracted total.

### 🚀 Enhancement

- Replaced the IPInfo API with the MaxMind GeoLite2-City database for IP geolocation. The database is downloaded into `[cache_directory]/geolite2/` on first use and refreshed automatically once it is more than a week old, using the `MAXMIND_ACCOUNT_ID` and `MAXMIND_LICENSE_KEY` credentials of a free MaxMind account; `IPINFO_API_KEY` is no longer read. Lookups are local, so there is no request quota, no per-IP network call, and no early halt on exhaustion. An address that is malformed, absent from the database, or without a country there is labeled `unknown`; a label is never `None`, and `update ip regions` rewrites any `None` entries left in the cache by earlier versions as `unknown`. The `is_cloud_service_or_vpn_label` and `is_resolved_region` helpers, and the summaries, also tolerate a `None` entry, which they treat as an unresolved location, instead of failing on it. Region labels now pair the ISO 3166-1 alpha-3 country code with the ISO 3166-2 subdivision code, as in `USA/CA` or `GBR/ENG`, rather than the alpha-2 country code with a free-text region name as in `US/California`. A code outside ISO 3166-1, such as the `XK` that geolocation databases use for Kosovo, is kept as-is since it has no alpha-3 form. Existing `ip_to_region.yaml` caches keep their old-style labels until re-resolved, either by deleting the cache file and re-running `update ip regions`, or over the 90-day cycle of `update ip refresh`. A new `s3logextraction update ip database` command (and `update_geolite2_database` API) downloads the database explicitly, with `--force` to refresh a copy that is not yet stale. The `geolocation` extra now installs `geoip2` and `requests` in place of `ipinfo`.

- Removed the OpenCage geocoder. `update ip coordinates` now looks each region label up in ISO 3166 tables bundled with the package, which give every country its alpha-2 and alpha-3 codes and a representative coordinate, and every ISO 3166-2 subdivision known to Natural Earth (public domain) a representative coordinate. Subdivisions that Natural Earth only maps at a finer level, such as England or Grand Est, get the mean of their constituent units' points via the ISO hierarchy, and a subdivision the tables do not know falls back to its country's point. The step no longer needs credentials or network access, `OPENCAGE_API_KEY` is no longer read, and the hand-maintained coordinate overrides that patched cross-country geocoding mismatches have been dropped. Cloud service regions such as `AWS/us-east-1` are located with the GeoLite2 database rather than IPInfo. Coordinates of existing regions will shift slightly when regenerated, since Natural Earth label points differ from OpenCage's results. The tables are regenerated with `tools/build_region_coordinates.py`.

- `number_of_unique_countries` in the totals now converts the alpha-2 prefix of AWS region names (`us-east-1`) to alpha-3 before counting, so that requesters located in a country and AWS regions in the same country are counted as one country.

- Reworked privacy protection around the `by_region.tsv` summaries. Individual values are no longer censored below a disclosure threshold or rounded to a modulo, so every summary now reports its true values. Protection instead gates the publication of `by_region.tsv`, which is the only summary that pairs activity with requester location. That file is written only when the update it carries moves more than `region_disclosure_threshold` (default `5`) resolved regions at once, and it is created for the first time only when its first update spans that many. A resolved region is any label naming a physical place, such as `US/California` or `AWS/us-east-1`. Labels such as `missing`, `undetermined`, `GitHub`, and `VPN` name no place and do not count towards privacy calculations. The consequence is that the totals of a `by_region.tsv` drift out of step with the other summaries between publications. The `privacy_threshold_minimum` argument of `generate_summaries`, `generate_archive_summaries`, `generate_all_dataset_totals`, and `generate_archive_totals` is replaced by `region_disclosure_threshold` on the first two and removed from the last two. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a public `is_resolved_region` helper to `ip_utils`, alongside the existing `is_cloud_service_or_vpn_label`, so that region labels can be classified from outside the summaries on the same terms the summaries use. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a `--threshold` flag to `s3logextraction update summaries`, in both the default and the `archive` mode, which sets the `region_disclosure_threshold` the by-region summaries are published under. It defaults to `5`. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- `totals.json` and `archive_totals.json` now read their activity totals from the by-day summaries rather than the by-region summaries, so that they stay true and in step with `by_day.tsv` and `by_asset.tsv` even while a `by_region.tsv` is withheld. Only `number_of_unique_regions` and `number_of_unique_countries` still come from the by-region summary, and both report `0` while it is withheld. `generate_archive_totals` raises a `FileNotFoundError` naming the archive by-day summary when it has not been generated. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a `number_of_views` column to the `by_asset.tsv`, `by_day.tsv`, and `by_region.tsv` summaries, per dataset and for the archive, along with `total_number_of_views` in `totals.json` and `archive_totals.json`. A view is a streaming session rather than a request, defined as a maximal run of streaming (HTTP 206) requests from one IP address to one asset in which no two consecutive requests are more than 8 hours apart. Full downloads (HTTP 200) are never views and continue to be reported by `number_of_downloads`. The threshold is the `SESSION_TIMEOUT_IN_SECONDS` constant. A session can straddle midnight, so it is counted on the day of its first request, and it is attributed to the region of the single requester that made it. View counts are privacy-rounded on the same modulo and minimum disclosure threshold as the request and download counts. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

- Summary generation now raises a `RuntimeError` when an asset's `timestamps.txt`, `download.txt`, or `ips.txt` is missing or when they are not line-aligned, instead of quietly counting that asset as having no views and no downloads. Line N of each file must describe the same request. A missing or short `download.txt` means the asset was extracted before that file existed, so the extraction cache is incompatible and the asset must be re-extracted. Any other case means the extraction cache is corrupted, most often by an extraction interrupted partway through writing these files. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

### 🏠 Internal

- Consolidated the module-level constants of the `summarize` submodule into a `globals.py`, which is public so that the disclosure threshold and the session timeout can be read from outside. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Added a third example log collection of repeated access from several documentation-range requesters, along with a mocked `ip_to_region` cache that stands in for a geolocation of them. The integration tests now cover a published `by_region.tsv` and summary values that accumulate over more than one request per asset, per day, and per region. ([#294](https://github.com/dandi/s3-log-extraction/pull/294))

- Sessionized each asset once per dataset summary and shared the result across the by-asset, by-day, and by-region tables, so `ips.txt` is decrypted no more often than before. ([#293](https://github.com/dandi/s3-log-extraction/pull/293))

- Added a `Version Check` CI workflow that fails pull requests which modify `src/` or `pyproject.toml` without bumping the package version. ([#292](https://github.com/dandi/s3-log-extraction/pull/292))

### 🐛 Bug Fix

- Fixed the archive requester count, which double-counted requesters. `generate_summaries` writes the archive `requester_count.tsv` as the number of unique IP addresses across the whole archive, and `generate_archive_summaries` then overwrote it with the sum of the per-dataset counts, counting a requester once per dataset it accessed. The archive summaries no longer write that file, leaving the deduplicated count in place. Running `update summaries --mode archive` without having run `update summaries` first now leaves the archive count absent rather than writing an inflated one, and `generate_archive_totals` names the dataset summaries as the step to run in the `FileNotFoundError` it raises for the missing file. Published `archive_totals.json` values of `number_of_requesters` were overstated by the amount of cross-dataset overlap and will drop when the archive summaries are next regenerated. ([#295](https://github.com/dandi/s3-log-extraction/pull/295))

- Fixed the daily IP cache update workflows so that an exhausted IPInfo (or OpenCage) API quota no longer wastes hours on doomed requests or crashes the run. `update ip regions` now halts at the first quota error and leaves unprocessed IPs uncached for retry on the next run, instead of permanently caching them as `undetermined`. `update ip coordinates` now saves partial progress and exits cleanly instead of raising an unhandled `RequestQuotaExceededError`. `update ip refresh` now halts early without overwriting existing cache entries. The daily remote tests skip (rather than fail) when the quota is exhausted, since live lookups cannot be validated in that state. ([#292](https://github.com/dandi/s3-log-extraction/pull/292))

## v1.10.8

### 🚀 Enhancement

- Excluded known cloud service and VPN IPs (GitHub, AWS, GCP, VPN) from the `number_of_requesters` count reported in per-dataset, archive, and total summaries. ([#291](https://github.com/dandi/s3-log-extraction/pull/291))
- Strengthened encryption key derivation. The `S3_LOG_EXTRACTION_PASSWORD` value now passes through PBKDF2-HMAC-SHA256 instead of a single SHA-256 pass, which resists brute-force attacks. Weak passwords are also rejected before any encryption runs. A public `validate_password_strength` helper was added. ([#283](https://github.com/dandi/s3-log-extraction/pull/283))
- Added the `s3logextraction stats` CLI command and the `get_log_bucket_stats` API helper for summarizing S3 inventory. ([#224](https://github.com/dandi/s3-log-extraction/pull/224))
- Extended the `s3logextraction stats` command and added a `get_ip_stats` API helper to report IP address classification statistics from the IP cache. Every cached IP is binned into one of seven categories (determined, missing, unknown, bogon, VPN, cloud service, GitHub) with counts and percentages. The command also gains `--cache` and `--encryption` flags. ([#274](https://github.com/dandi/s3-log-extraction/pull/274))

### 🐛 Bug Fix

- Narrowed `is_cloud_service_or_vpn_label` to only match genuine cloud/VPN service labels (`"GitHub"`, `"VPN"`, `"AWS/…"`, `"GCP/…"`). It previously also matched unresolved-location labels (`"unknown"`, `"undetermined"`, `"missing"`, `"bogon"`), which caused real requesters whose IPs could not be geolocated to be silently dropped from `number_of_requesters`. ([#291](https://github.com/dandi/s3-log-extraction/pull/291))
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

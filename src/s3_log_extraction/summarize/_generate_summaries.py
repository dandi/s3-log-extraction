import collections
import datetime
import json
import pathlib
import typing
import warnings

import pandas
import tqdm

from ._globals import (
    ASSET_METRIC_COLUMN_NAMES,
    MINIMUM_REGIONS_FOR_DISCLOSURE,
    PRIVACY_ROUNDED_COLUMN_NAMES,
    SESSION_TIMEOUT_SECONDS,
    SUMMARY_UPDATE_INTERVAL_DAYS,
    TIMESTAMP_FORMAT,
)
from ..config import get_cache_directory, get_cache_subdirectory
from ..ip_utils import load_ip_cache
from ..ip_utils._globals import EXCLUDED_REGION_LABELS, is_cloud_service_or_vpn_label
from ..ip_utils._ip_utils import _read_ips_from_file


def _round_requester_count(count: int, modulo: int, minimum: int) -> str | int:
    """
    Round a unique requester count for privacy protection.

    If the count is less than ``minimum``, returns a sentinel string indicating
    the count is below the threshold (e.g., ``"<50"``).  Otherwise, rounds to
    the nearest multiple of ``modulo``.

    Parameters
    ----------
    count : int
        The exact number of unique requesters to round.
    modulo : int
        The granularity used for rounding (e.g., ``20`` rounds to the nearest 20).
    minimum : int
        The minimum disclosure threshold.  Counts below this value are reported
        as ``"<{minimum}"`` to protect privacy.

    Returns
    -------
    str or int
        A string of the form ``"<{minimum}"`` if ``count < minimum``, otherwise
        an integer rounded to the nearest multiple of ``modulo``.
    """
    if count < minimum:
        return f"<{minimum}"
    return round(count / modulo) * modulo


def _read_privacy_rounded_count(*, summary_file_path: pathlib.Path, privacy_threshold_minimum: int = 50) -> str | int:
    """
    Read a single already privacy-rounded count from a summary file.

    Returns the censored sentinel when the file is missing, which is how a cache summarized by an older
    version of this package reports a count it never generated.
    """
    if not summary_file_path.exists():
        return f"<{privacy_threshold_minimum}"

    value = summary_file_path.read_text().strip()
    return value if value.startswith("<") else int(value)


def _privacy_round_request_download_columns(
    summary_table: pandas.DataFrame, *, modulo: int = 20, minimum: int = 50
) -> pandas.DataFrame:
    """
    Apply privacy rounding to every access count column present in the summary table.

    Only the by-day and by-region summaries are rounded this way. The per-asset summaries are protected
    by the region-diversity gate of :func:`_release_asset_row` instead.
    """
    for column_name in PRIVACY_ROUNDED_COLUMN_NAMES:
        summary_table[column_name] = summary_table[column_name].map(
            lambda count: _round_requester_count(count=int(count), modulo=modulo, minimum=minimum)
        )
    return summary_table


class AssetAccessSummary(typing.NamedTuple):
    """The exact access metrics of a single asset, together with the evidence needed to release them."""

    bytes_sent: int
    number_of_requests: int
    number_of_downloads: int
    number_of_views: int
    recent_regions: frozenset[str]


def _parse_timestamps(timestamps: list[str]) -> list[float]:
    """Parse extraction cache timestamps into epoch seconds, anchored to UTC so results are reproducible."""
    return [
        datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).replace(tzinfo=datetime.timezone.utc).timestamp()
        for timestamp in timestamps
    ]


def _count_sessions(epoch_seconds: list[float], /, session_timeout_seconds: int = SESSION_TIMEOUT_SECONDS) -> int:
    """Count the maximal runs of one IP's requests separated by no more than ``session_timeout_seconds``."""
    if not epoch_seconds:
        return 0

    ordered = sorted(epoch_seconds)
    return 1 + sum(1 for previous, current in zip(ordered, ordered[1:]) if current - previous > session_timeout_seconds)


def _summarize_asset_access(
    *,
    asset_directory: pathlib.Path,
    ip_to_region: dict[str, str],
    window_start: float,
    use_encryption: bool = True,
    session_timeout_seconds: int = SESSION_TIMEOUT_SECONDS,
) -> AssetAccessSummary | None:
    """
    Read every per-request file of one asset in a single pass and reduce it to that asset's access metrics.

    The ``number_of_views`` metric counts streaming sessions rather than requests. One person exploring one
    file over a remote connection emits hundreds to thousands of partial range requests, so raw request
    counts measure a mix of interest and the mechanical cost of reading the file. A view is therefore a
    maximal run of streaming requests from one IP address to this asset in which no two consecutive
    requests are more than ``session_timeout_seconds`` apart. Only streaming requests count, which the
    extraction cache marks with a ``0`` in ``download.txt``. Full downloads are counted separately.

    ``recent_regions`` holds the genuine geographic regions that accessed this asset at or after
    ``window_start``. It is the evidence used to decide whether this asset's row may be republished, and
    is never itself published.

    Parameters
    ----------
    asset_directory : pathlib.Path
        Path to a per-asset extraction directory holding the line-aligned per-request files.
    ip_to_region : dict of str to str
        Mapping of IP address to region/service label.
    window_start : float
        Epoch seconds marking the start of the reporting week.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` is decrypted before reading.
        If ``False``, the file is read as plaintext.
    session_timeout_seconds : int
        Maximum gap between two consecutive streaming requests of the same view.
        Defaults to ``SESSION_TIMEOUT_SECONDS`` (8 hours).

    Returns
    -------
    AssetAccessSummary or None
        The asset's exact metrics, or ``None`` when it has no ``bytes_sent.txt`` to summarize.
    """
    bytes_sent_file_path = asset_directory / "bytes_sent.txt"
    if not bytes_sent_file_path.exists():
        return None

    bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
    number_of_requests = len(bytes_sent)

    download_file_path = asset_directory / "download.txt"
    if download_file_path.exists():
        downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
    else:
        downloads = [0] * number_of_requests

    timestamps_file_path = asset_directory / "timestamps.txt"
    ips_file_path = asset_directory / "ips.txt"
    if not (timestamps_file_path.exists() and ips_file_path.exists()):
        return AssetAccessSummary(
            bytes_sent=sum(bytes_sent),
            number_of_requests=number_of_requests,
            number_of_downloads=sum(downloads),
            number_of_views=0,
            recent_regions=frozenset(),
        )

    timestamps = [stripped for line in timestamps_file_path.read_text().splitlines() if (stripped := line.strip())]
    ips = _read_ips_from_file(file_path=ips_file_path, use_encryption=use_encryption)

    if not len(timestamps) == len(downloads) == len(ips):
        message = (
            f"\nSkipping view and region counting for '{asset_directory}' due to mismatched line counts "
            f"(timestamps: {len(timestamps)}, downloads: {len(downloads)}, IPs: {len(ips)}).\n"
        )
        warnings.warn(message=message, stacklevel=2)
        return AssetAccessSummary(
            bytes_sent=sum(bytes_sent),
            number_of_requests=number_of_requests,
            number_of_downloads=sum(downloads),
            number_of_views=0,
            recent_regions=frozenset(),
        )

    epoch_seconds = _parse_timestamps(timestamps)

    streaming_epoch_seconds_per_ip = collections.defaultdict(list)
    recent_regions: set[str] = set()
    for epoch_second, download, ip in zip(epoch_seconds, downloads, ips):
        if download == 0:  # Full downloads are not views
            streaming_epoch_seconds_per_ip[ip].append(epoch_second)

        if epoch_second >= window_start:
            region = ip_to_region.get(ip, "missing")
            if region not in EXCLUDED_REGION_LABELS and not is_cloud_service_or_vpn_label(region):
                recent_regions.add(region)

    number_of_views = sum(
        _count_sessions(ip_epoch_seconds, session_timeout_seconds=session_timeout_seconds)
        for ip_epoch_seconds in streaming_epoch_seconds_per_ip.values()
    )

    return AssetAccessSummary(
        bytes_sent=sum(bytes_sent),
        number_of_requests=number_of_requests,
        number_of_downloads=sum(downloads),
        number_of_views=number_of_views,
        recent_regions=frozenset(recent_regions),
    )


def _release_asset_row(
    recent_regions: frozenset[str], /, minimum_regions: int = MINIMUM_REGIONS_FOR_DISCLOSURE
) -> bool:
    """
    Decide whether this week's exact values for an asset may be released.

    An update is released only when more than ``minimum_regions`` genuine geographic regions accessed the
    asset during the reporting week. The exact counts always remain in the extraction cache; this decides
    only whether the published summary is refreshed with them or left showing the previous release.
    """
    return len(recent_regions) > minimum_regions


def _read_previous_asset_rows(summary_file_path: pathlib.Path, /) -> dict[str, dict[str, str]]:
    """
    Read the values published for each asset by the previous release.

    Withheld assets keep exactly these values, so that a sequence of releases never exposes the activity
    of the few requesters seen in between.
    """
    if not summary_file_path.exists():
        return {}

    previous_table = pandas.read_table(filepath_or_buffer=summary_file_path, dtype=str)
    if "asset_path" not in previous_table.columns:
        return {}

    return {
        str(row["asset_path"]): {
            column_name: str(row[column_name])
            for column_name in ASSET_METRIC_COLUMN_NAMES
            if column_name in previous_table.columns
        }
        for _, row in previous_table.iterrows()
    }


def _read_summary_update_state(summary_directory: pathlib.Path, /) -> datetime.datetime | None:
    """Read the moment the per-asset summaries were last released, or ``None`` if they never were."""
    state_file_path = summary_directory / "summary_update_state.json"
    if not state_file_path.exists():
        return None

    state = json.loads(state_file_path.read_text())
    last_updated = state.get("last_updated", None)
    return datetime.datetime.fromisoformat(last_updated) if last_updated is not None else None


def _write_summary_update_state(*, summary_directory: pathlib.Path, reference_datetime: datetime.datetime) -> None:
    """Record the moment of this release, which starts the clock on the next permitted one."""
    summary_directory.mkdir(parents=True, exist_ok=True)
    state_file_path = summary_directory / "summary_update_state.json"
    state_file_path.write_text(json.dumps(obj={"last_updated": reference_datetime.isoformat()}, indent=2))


def _assert_update_interval_elapsed(
    *,
    summary_directory: pathlib.Path,
    reference_datetime: datetime.datetime,
    interval_days: int = SUMMARY_UPDATE_INTERVAL_DAYS,
) -> None:
    """
    Raise unless a full update interval has passed since the previous release.

    Aggregate access counts are only safe to publish once per period of activity. Releasing them more
    often lets an observer subtract two consecutive releases and recover the behavior of the handful of
    requesters active in between, which no per-value rounding can prevent.
    """
    last_updated = _read_summary_update_state(summary_directory)
    if last_updated is None:
        return

    next_permitted = last_updated + datetime.timedelta(days=interval_days)
    if reference_datetime >= next_permitted:
        return

    message = (
        f"\n\nThe summaries were last released on {last_updated.isoformat()} and may not be released again "
        f"until {next_permitted.isoformat()} ({interval_days} days later).\n"
        "Publishing access counts more often than once per interval allows consecutive releases to be "
        "differenced, which exposes the small number of requesters active in between.\n"
        "The exact counts remain available in the extraction cache in the meantime.\n\n"
    )
    raise RuntimeError(message)


def _collect_unique_ips(
    asset_directories: list[pathlib.Path],
    use_encryption: bool = True,
    ip_to_region: dict[str, str] | None = None,
) -> set[str]:
    """
    Collect all unique IP addresses across the given asset directories.

    Parameters
    ----------
    asset_directories : list of pathlib.Path
        Paths to per-asset extraction directories containing ``ips.txt`` files.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        If ``False``, files are read as plaintext.
    ip_to_region : dict of str to str, optional
        Mapping of IP address to region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the collected set. If not
        provided, no exclusion is applied.

    Returns
    -------
    set of str
        The set of unique IP addresses found across all ``ips.txt`` files, excluding
        any IPs classified as a known cloud service or VPN.
    """
    ip_to_region = ip_to_region or {}
    unique_ips: set[str] = set()
    for asset_directory in asset_directories:
        full_ips_file_path = asset_directory / "ips.txt"
        if not full_ips_file_path.exists():
            continue
        ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        unique_ips.update(ip for ip in ips if not is_cloud_service_or_vpn_label(ip_to_region.get(ip, "")))
    return unique_ips


def _summarize_dataset_requester_count(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    ip_to_region: dict[str, str],
    modulo: int = 20,
    minimum: int = 50,
    use_encryption: bool = True,
) -> None:
    """
    Compute and save the privacy-rounded unique requester count for a dataset.

    Reads all ``ips.txt`` files from the given asset directories, counts the
    number of unique IP addresses across the entire dataset (excluding known cloud
    service and VPN IPs), rounds the result via :func:`_round_requester_count`, and
    writes the value to ``summary_file_path``.

    Parameters
    ----------
    asset_directories : list of pathlib.Path
        Paths to the per-asset extraction directories containing ``ips.txt`` files.
    summary_file_path : pathlib.Path
        Destination file where the rounded count (as a string) will be written.
    ip_to_region : dict of str to str
        Mapping of IP address to region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the requester count.
    modulo : int, optional
        Granularity for rounding.  Default is ``20``.
    minimum : int, optional
        Minimum disclosure threshold.  Counts below this are reported as ``"<{minimum}"``.
        Default is ``50``.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        If ``False``, files are read as plaintext.
    """
    unique_ips = _collect_unique_ips(
        asset_directories=asset_directories, use_encryption=use_encryption, ip_to_region=ip_to_region
    )

    if not unique_ips:
        return

    rounded_count = _round_requester_count(count=len(unique_ips), modulo=modulo, minimum=minimum)
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(rounded_count))


def generate_summaries(
    level: int = 0,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
    reference_datetime: datetime.datetime | None = None,
) -> None:
    """
    Generate summaries for each dataset in the extraction directory.

    There are several TSV summary files generated per outer level of the S3 bucket structure:
        - `by_day.tsv`: Summarizes the total bytes sent per day across all assets in the dataset.
        - `by_asset.tsv`: Summarizes the total bytes sent per asset in the dataset.
        - `by_region.tsv`: Summarizes the total bytes sent per region based on geolocations of the indexed IPs.

    Parameters
    ----------
    level : int
        The level of summaries to generate.
        Currently only level 0 is supported, which generates summaries for each dataset.
        Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` and IP cache files are decrypted when read.
        If ``False``, files are read as plaintext.
    privacy_threshold_minimum : int
        Minimum disclosure threshold for privacy-rounded requester/request/download
        values. Default is ``50``.
    reference_datetime : datetime.datetime, optional
        The moment this release is considered to happen, defaulting to now in UTC.
        The reporting week is the ``SUMMARY_UPDATE_INTERVAL_DAYS`` ending here.

    Raises
    ------
    RuntimeError
        If fewer than ``SUMMARY_UPDATE_INTERVAL_DAYS`` have passed since the previous release.
    """
    if level != 0:
        message = (
            "\n\nCurrently only level 0 summaries are supported."
            "Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new\n\n"
        )
        raise NotImplementedError(message)

    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_dir / "extraction"
    extraction_directory.mkdir(exist_ok=True)
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")

    release_datetime = reference_datetime or datetime.datetime.now(tz=datetime.timezone.utc)
    _assert_update_interval_elapsed(summary_directory=summary_directory, reference_datetime=release_datetime)
    window_start = (release_datetime - datetime.timedelta(days=SUMMARY_UPDATE_INTERVAL_DAYS)).timestamp()

    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )

    datasets = [item for item in extraction_directory.iterdir() if item.is_dir()]
    all_archive_unique_ips: set[str] = set()
    for dataset in tqdm.tqdm(
        iterable=datasets,
        total=len(datasets),
        desc="Summarizing Datasets",
        position=0,
        leave=True,
        mininterval=5.0,
        smoothing=0,
        unit="dataset",
    ):
        dataset_id = dataset.name

        asset_directories = sorted([file_path.parent for file_path in dataset.rglob(pattern="*bytes_sent.txt")])
        _summarize_dataset(
            dataset_id=dataset_id,
            asset_directories=asset_directories,
            summary_directory=summary_directory,
            ip_to_region=ip_to_region,
            window_start=window_start,
            use_encryption=use_encryption,
            privacy_threshold_minimum=privacy_threshold_minimum,
        )

        all_archive_unique_ips.update(
            _collect_unique_ips(
                asset_directories=asset_directories, use_encryption=use_encryption, ip_to_region=ip_to_region
            )
        )
    if all_archive_unique_ips:
        archive_directory = summary_directory / "archive"
        archive_directory.mkdir(exist_ok=True)
        rounded_archive_count = _round_requester_count(
            count=len(all_archive_unique_ips), modulo=20, minimum=privacy_threshold_minimum
        )
        (archive_directory / "requester_count.tsv").write_text(str(rounded_archive_count))

    # Written only after a successful pass, so that a crashed run does not lock out the next attempt
    _write_summary_update_state(summary_directory=summary_directory, reference_datetime=release_datetime)


def _summarize_dataset(
    *,
    dataset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    ip_to_region: dict[str, str],
    window_start: float,
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
) -> None:
    _summarize_dataset_by_day(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_day.tsv",
        privacy_threshold_minimum=privacy_threshold_minimum,
    )
    total_number_of_views, dataset_recent_regions = _summarize_dataset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_asset.tsv",
        ip_to_region=ip_to_region,
        window_start=window_start,
        use_encryption=use_encryption,
        privacy_threshold_minimum=privacy_threshold_minimum,
    )
    _write_dataset_view_count(
        total_number_of_views=total_number_of_views,
        recent_regions=dataset_recent_regions,
        summary_file_path=summary_directory / dataset_id / "view_count.tsv",
        minimum=privacy_threshold_minimum,
    )
    _summarize_dataset_by_region(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_region.tsv",
        ip_to_region=ip_to_region,
        use_encryption=use_encryption,
        privacy_threshold_minimum=privacy_threshold_minimum,
    )
    _summarize_dataset_requester_count(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "requester_count.tsv",
        ip_to_region=ip_to_region,
        minimum=privacy_threshold_minimum,
        use_encryption=use_encryption,
    )


def _summarize_dataset_by_day(
    *, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path, privacy_threshold_minimum: int = 50
) -> None:
    all_dates = []
    all_bytes_sent = []
    all_downloads = []
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        timestamps_file_path = asset_directory / "timestamps.txt"

        if not timestamps_file_path.exists():
            continue

        dates = [
            datetime.datetime.strptime(str(timestamp.strip()), "%y%m%d%H%M%S").strftime(format="%Y-%m-%d")
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        if download_file_path.exists():
            downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
        else:
            downloads = [0] * len(dates)
        all_downloads.extend(downloads)

    summarized_activity_by_day = collections.defaultdict(int)
    number_of_requests_by_day = collections.defaultdict(int)
    number_of_downloads_by_day = collections.defaultdict(int)
    for date, bytes_sent, download in zip(all_dates, all_bytes_sent, all_downloads):
        summarized_activity_by_day[date] += bytes_sent
        number_of_requests_by_day[date] += 1
        number_of_downloads_by_day[date] += download

    if len(summarized_activity_by_day) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_dates_ordered = list(summarized_activity_by_day.keys())
    summary_table = pandas.DataFrame(
        data={
            "date": all_dates_ordered,
            "bytes_sent": list(summarized_activity_by_day.values()),
            "number_of_requests": [number_of_requests_by_day[date] for date in all_dates_ordered],
            "number_of_downloads": [number_of_downloads_by_day[date] for date in all_dates_ordered],
        }
    )
    summary_table.sort_values(by="date", inplace=True)
    summary_table.index = range(len(summary_table))
    summary_table = _privacy_round_request_download_columns(
        summary_table=summary_table, minimum=privacy_threshold_minimum
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _write_dataset_view_count(
    *,
    total_number_of_views: int,
    recent_regions: frozenset[str],
    summary_file_path: pathlib.Path,
    modulo: int = 20,
    minimum: int = 50,
) -> None:
    """
    Save the total number of views across all assets of a dataset.

    The exact total is released when more than ``MINIMUM_REGIONS_FOR_DISCLOSURE`` genuine geographic
    regions accessed the dataset during the reporting week. Otherwise the previously released value is
    left in place, or, if there is none, a conservatively rounded value is released instead.
    """
    if not _release_asset_row(recent_regions):
        if summary_file_path.exists():
            return

        rounded_count = _round_requester_count(count=total_number_of_views, modulo=modulo, minimum=minimum)
        summary_file_path.parent.mkdir(parents=True, exist_ok=True)
        summary_file_path.write_text(str(rounded_count))
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(total_number_of_views))


def _summarize_dataset_by_asset(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    ip_to_region: dict[str, str],
    window_start: float,
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
) -> tuple[int, frozenset[str]]:
    """
    Summarize per-asset activity for a dataset, releasing only the rows that clear the disclosure gate.

    Each asset's exact metrics are computed from the extraction cache every run. Whether they reach the
    published table is a separate decision: an asset's row is refreshed only when more than
    ``MINIMUM_REGIONS_FOR_DISCLOSURE`` genuine geographic regions accessed it during the reporting week.
    A withheld asset keeps the values of its previous release verbatim, so consecutive releases cannot be
    differenced to expose the few requesters seen in between. An asset that has never been released and
    does not yet qualify is published with conservatively rounded values.

    Returns the exact total number of views across the dataset and the genuine geographic regions that
    accessed any of its assets during the reporting week.
    """
    dataset_id = summary_file_path.parent.name
    extraction_base_path = summary_file_path.parent.parent.parent / "extraction" / dataset_id  # Assumes same cache dir

    previous_rows = _read_previous_asset_rows(summary_file_path)

    released_rows: dict[str, dict[str, str | int]] = {}
    total_number_of_views = 0
    dataset_recent_regions: set[str] = set()
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        asset_summary = _summarize_asset_access(
            asset_directory=asset_directory,
            ip_to_region=ip_to_region,
            window_start=window_start,
            use_encryption=use_encryption,
        )
        if asset_summary is None:
            continue

        asset_path = str(asset_directory.relative_to(extraction_base_path))
        total_number_of_views += asset_summary.number_of_views
        dataset_recent_regions.update(asset_summary.recent_regions)

        exact_row = {
            "bytes_sent": asset_summary.bytes_sent,
            "number_of_requests": asset_summary.number_of_requests,
            "number_of_downloads": asset_summary.number_of_downloads,
            "number_of_views": asset_summary.number_of_views,
        }
        if _release_asset_row(asset_summary.recent_regions):
            released_rows[asset_path] = exact_row
        elif asset_path in previous_rows:
            released_rows[asset_path] = dict(previous_rows[asset_path])
        else:
            released_rows[asset_path] = {
                "bytes_sent": asset_summary.bytes_sent,
                **{
                    column_name: _round_requester_count(
                        count=exact_row[column_name], modulo=20, minimum=privacy_threshold_minimum
                    )
                    for column_name in ("number_of_requests", "number_of_downloads", "number_of_views")
                },
            }

    if len(released_rows) == 0:
        return total_number_of_views, frozenset(dataset_recent_regions)

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_asset_paths = list(released_rows.keys())
    summary_table = pandas.DataFrame(
        data={
            "asset_path": all_asset_paths,
            **{
                column_name: [released_rows[asset_path][column_name] for asset_path in all_asset_paths]
                for column_name in ASSET_METRIC_COLUMN_NAMES
            },
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

    return total_number_of_views, frozenset(dataset_recent_regions)


def _summarize_dataset_by_region(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    ip_to_region: dict[str, str],
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
) -> None:
    all_regions = []
    all_bytes_sent = []
    all_downloads = []
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        full_ips_file_path = asset_directory / "ips.txt"

        if not full_ips_file_path.exists():
            continue

        full_ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        regions = [ip_to_region.get(ip, "missing") for ip in full_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        if download_file_path.exists():
            downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
        else:
            downloads = [0] * len(regions)
        all_downloads.extend(downloads)

    summarized_activity_by_region = collections.defaultdict(int)
    number_of_requests_by_region = collections.defaultdict(int)
    number_of_downloads_by_region = collections.defaultdict(int)
    for region, bytes_sent, download in zip(all_regions, all_bytes_sent, all_downloads):
        summarized_activity_by_region[region] += bytes_sent
        number_of_requests_by_region[region] += 1
        number_of_downloads_by_region[region] += download

    if len(summarized_activity_by_region) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_regions_ordered = list(summarized_activity_by_region.keys())
    summary_table = pandas.DataFrame(
        data={
            "region": all_regions_ordered,
            "bytes_sent": list(summarized_activity_by_region.values()),
            "number_of_requests": [number_of_requests_by_region[region] for region in all_regions_ordered],
            "number_of_downloads": [number_of_downloads_by_region[region] for region in all_regions_ordered],
        }
    )
    summary_table = _privacy_round_request_download_columns(
        summary_table=summary_table, minimum=privacy_threshold_minimum
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

import collections
import datetime
import pathlib
import warnings

import pandas
import tqdm

from ._globals import PRIVACY_PROTECTED_COLUMN_NAMES, SESSION_TIMEOUT_SECONDS, TIMESTAMP_FORMAT
from ..config import get_cache_directory, get_cache_subdirectory
from ..ip_utils import load_ip_cache
from ..ip_utils._globals import is_cloud_service_or_vpn_label
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

    Columns listed in ``PRIVACY_PROTECTED_COLUMN_NAMES`` are rounded when present. Not every summary
    carries every column, so absent columns are skipped. For example, ``number_of_views`` is only
    reported per asset, since a streaming session is defined per requester and asset and therefore
    cannot be attributed to a single day or region.
    """
    for column_name in PRIVACY_PROTECTED_COLUMN_NAMES:
        if column_name not in summary_table.columns:
            continue

        summary_table[column_name] = summary_table[column_name].map(
            lambda count: _round_requester_count(count=int(count), modulo=modulo, minimum=minimum)
        )
    return summary_table


def _count_asset_views(
    *,
    asset_directory: pathlib.Path,
    use_encryption: bool = True,
    session_timeout_seconds: int = SESSION_TIMEOUT_SECONDS,
) -> int:
    """
    Count the number of views of a single asset.

    A view is a streaming session, not a request. One person exploring one file over a remote connection
    emits hundreds to thousands of partial range requests, so raw request counts measure a mix of interest
    and the mechanical cost of reading the file. Collapsing each burst of requests into a single countable
    unit is the fair measure of interest.

    A session is a maximal run of streaming requests from one IP address to this asset in which no two
    consecutive requests are more than ``session_timeout_seconds`` apart. Only streaming requests count,
    which the extraction cache marks with a ``0`` in ``download.txt``. Full downloads are reported
    separately by ``number_of_downloads``.

    Parameters
    ----------
    asset_directory : pathlib.Path
        Path to a per-asset extraction directory containing the line-aligned ``timestamps.txt``,
        ``download.txt``, and ``ips.txt`` files.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` is decrypted before reading.
        If ``False``, the file is read as plaintext.
    session_timeout_seconds : int
        Maximum gap between two consecutive streaming requests of the same session.
        Defaults to ``SESSION_TIMEOUT_SECONDS`` (8 hours).

    Returns
    -------
    int
        The total number of streaming sessions across all IP addresses that streamed this asset.
    """
    timestamps_file_path = asset_directory / "timestamps.txt"
    download_file_path = asset_directory / "download.txt"
    ips_file_path = asset_directory / "ips.txt"
    if not (timestamps_file_path.exists() and download_file_path.exists() and ips_file_path.exists()):
        return 0

    timestamps = [stripped for line in timestamps_file_path.read_text().splitlines() if (stripped := line.strip())]
    downloads = [stripped for line in download_file_path.read_text().splitlines() if (stripped := line.strip())]
    ips = _read_ips_from_file(file_path=ips_file_path, use_encryption=use_encryption)

    if not len(timestamps) == len(downloads) == len(ips):
        message = (
            f"\nSkipping view counting for '{asset_directory}' due to mismatched line counts "
            f"(timestamps: {len(timestamps)}, downloads: {len(downloads)}, IPs: {len(ips)}).\n"
        )
        warnings.warn(message=message, stacklevel=2)
        return 0

    epoch_seconds_per_ip = collections.defaultdict(list)
    for timestamp, download, ip in zip(timestamps, downloads, ips):
        if download != "0":  # Full downloads are not views
            continue

        parsed_timestamp = datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).replace(tzinfo=datetime.timezone.utc)
        epoch_seconds_per_ip[ip].append(parsed_timestamp.timestamp())

    number_of_views = 0
    for epoch_seconds in epoch_seconds_per_ip.values():
        epoch_seconds.sort()
        number_of_views += 1 + sum(
            1
            for previous, current in zip(epoch_seconds, epoch_seconds[1:])
            if current - previous > session_timeout_seconds
        )
    return number_of_views


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


def _summarize_dataset(
    *,
    dataset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    ip_to_region: dict[str, str],
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
) -> None:
    _summarize_dataset_by_day(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_day.tsv",
        privacy_threshold_minimum=privacy_threshold_minimum,
    )
    total_number_of_views = _summarize_dataset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_asset.tsv",
        use_encryption=use_encryption,
        privacy_threshold_minimum=privacy_threshold_minimum,
    )
    _write_dataset_view_count(
        total_number_of_views=total_number_of_views,
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
    *, total_number_of_views: int, summary_file_path: pathlib.Path, modulo: int = 20, minimum: int = 50
) -> None:
    """
    Save the privacy-rounded total number of views across all assets of a dataset.

    The per-asset view counts in ``by_asset.tsv`` are individually rounded and censored, so they cannot be
    summed into a meaningful dataset total. This rounds the exact total instead, mirroring how the unique
    requester count is reported.
    """
    rounded_count = _round_requester_count(count=total_number_of_views, modulo=modulo, minimum=minimum)
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(rounded_count))


def _summarize_dataset_by_asset(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    use_encryption: bool = True,
    privacy_threshold_minimum: int = 50,
) -> int:
    """
    Summarize per-asset activity for a dataset and return the exact total number of views.

    The returned total is not privacy-rounded, unlike the per-asset values written to the summary file.
    It is intended only as the input to the dataset-level rounding performed by
    :func:`_write_dataset_view_count`.
    """
    dataset_id = summary_file_path.parent.name
    extraction_base_path = summary_file_path.parent.parent.parent / "extraction" / dataset_id  # Assumes same cache dir

    summarized_activity_by_asset = collections.defaultdict(int)
    number_of_requests_by_asset = collections.defaultdict(int)
    number_of_downloads_by_asset = collections.defaultdict(int)
    number_of_views_by_asset = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        bytes_sent_file_path = asset_directory / "bytes_sent.txt"

        if not bytes_sent_file_path.exists():
            continue

        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        asset_path = str(asset_directory.relative_to(extraction_base_path))
        summarized_activity_by_asset[asset_path] += sum(bytes_sent)
        number_of_requests_by_asset[asset_path] += len(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        if download_file_path.exists():
            downloads = [int(value.strip()) for value in download_file_path.read_text().splitlines()]
            number_of_downloads_by_asset[asset_path] += sum(downloads)
        else:
            number_of_downloads_by_asset[asset_path] += 0

        number_of_views_by_asset[asset_path] += _count_asset_views(
            asset_directory=asset_directory, use_encryption=use_encryption
        )

    total_number_of_views = sum(number_of_views_by_asset.values())
    if len(summarized_activity_by_asset) == 0:
        return total_number_of_views

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    all_asset_paths = list(summarized_activity_by_asset.keys())
    summary_table = pandas.DataFrame(
        data={
            "asset_path": all_asset_paths,
            "bytes_sent": list(summarized_activity_by_asset.values()),
            "number_of_requests": [number_of_requests_by_asset[path] for path in all_asset_paths],
            "number_of_downloads": [number_of_downloads_by_asset[path] for path in all_asset_paths],
            "number_of_views": [number_of_views_by_asset[path] for path in all_asset_paths],
        }
    )
    summary_table = _privacy_round_request_download_columns(
        summary_table=summary_table, minimum=privacy_threshold_minimum
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

    return total_number_of_views


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

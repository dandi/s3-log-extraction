import collections
import datetime
import pathlib

import pandas
import tqdm

from ._utils import _write_summary_by_region
from .globals import (
    REGION_DISCLOSURE_THRESHOLD,
    SESSION_TIMEOUT_IN_SECONDS,
    TIMESTAMP_FORMAT,
)
from ..config import get_cache_directory, get_cache_subdirectory
from ..ip_utils import (
    IpRegionResolver,
    RegionResolver,
    is_cloud_service_or_vpn_label,
)
from ..ip_utils._ip_utils import _read_ips_from_file


def _read_integers_from_file(file_path: pathlib.Path, /) -> list[int]:
    """Read one integer per line from an extraction file such as ``bytes_sent.txt`` or ``download.txt``."""
    return [int(value.strip()) for value in file_path.read_text().splitlines()]


def _collect_asset_views(
    *,
    asset_directory: pathlib.Path,
    use_encryption: bool = True,
    session_timeout_in_seconds: int = SESSION_TIMEOUT_IN_SECONDS,
) -> list[tuple[str, str]]:
    """
    Collect the views of a single asset.

    A view is a streaming session, not a request. One person exploring one file over a remote connection
    emits hundreds to thousands of partial range requests, so raw request counts measure a mix of interest
    and the mechanical cost of reading the file. Collapsing each burst of requests into a single countable
    unit is the fair measure of interest.

    A session is a maximal run of streaming requests from one IP address to this asset in which no two
    consecutive requests are more than ``session_timeout_in_seconds`` apart. Only streaming requests count,
    which the extraction cache marks with a ``0`` in ``download.txt``. Full downloads are reported
    separately by ``number_of_downloads``.

    Each view is returned with the date it began and the IP that made it, so that a view can be attributed
    to a single day and a single region. A session spans requests and can straddle midnight, so it is
    counted on the day of its first request.

    Parameters
    ----------
    asset_directory : pathlib.Path
        Path to a per-asset extraction directory containing the line-aligned ``timestamps.txt``,
        ``download.txt``, and ``ips.txt`` files.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` is decrypted before reading.
        If ``False``, the file is read as plaintext.
    session_timeout_in_seconds : int
        Maximum gap between two consecutive streaming requests of the same session.
        Defaults to ``SESSION_TIMEOUT_IN_SECONDS`` (8 hours).

    Returns
    -------
    list of tuple of str
        One ``(date, ip)`` pair per view, where ``date`` is the ``YYYY-MM-DD`` day the session began.

    Raises
    ------
    RuntimeError
        If any per-request file is missing, or if they are not line-aligned. Either means the extraction
        cache is incompatible (extracted before ``download.txt`` was introduced) or corrupted.
    """
    timestamps_file_path = asset_directory / "timestamps.txt"
    download_file_path = asset_directory / "download.txt"
    ips_file_path = asset_directory / "ips.txt"
    missing_file_names = [
        file_path.name
        for file_path in (timestamps_file_path, download_file_path, ips_file_path)
        if not file_path.exists()
    ]
    if missing_file_names:
        message = (
            f"\n\nThe extracted files for '{asset_directory}' are incomplete: "
            f"{', '.join(missing_file_names)} not found.\n"
            "Extraction writes every per-request file of an asset together, so none of them should be absent.\n\n"
            "A missing 'download.txt' means the asset was extracted before that file was introduced, which makes "
            "the extraction cache incompatible. Re-extract the asset to resolve it.\n"
            "Any other missing file means the extraction cache is corrupted.\n\n"
        )
        raise RuntimeError(message)

    timestamps = [stripped for line in timestamps_file_path.read_text().splitlines() if (stripped := line.strip())]
    downloads = [stripped for line in download_file_path.read_text().splitlines() if (stripped := line.strip())]
    ips = _read_ips_from_file(file_path=ips_file_path, use_encryption=use_encryption)

    if not len(timestamps) == len(downloads) == len(ips):
        message = (
            f"\n\nThe extracted files for '{asset_directory}' are not line-aligned "
            f"(timestamps: {len(timestamps)}, downloads: {len(downloads)}, IPs: {len(ips)}).\n"
            "Line N of each file must describe the same request for views to be counted.\n\n"
            "A short 'download.txt' means the asset was extracted before that file was introduced, which makes "
            "the extraction cache incompatible. Re-extract the asset to resolve it.\n"
            "Any other mismatch means the extraction cache is corrupted, most often by an extraction that was "
            "interrupted partway through writing these files.\n\n"
        )
        raise RuntimeError(message)

    parsed_timestamps_per_ip = collections.defaultdict(list)
    for timestamp, download, ip in zip(timestamps, downloads, ips):
        if download != "0":  # Full downloads are not views
            continue

        parsed_timestamps_per_ip[ip].append(
            datetime.datetime.strptime(timestamp, TIMESTAMP_FORMAT).replace(tzinfo=datetime.timezone.utc)
        )

    views: list[tuple[str, str]] = []
    for ip, parsed_timestamps in parsed_timestamps_per_ip.items():
        parsed_timestamps.sort()
        session_starts = [parsed_timestamps[0]] + [
            current
            for previous, current in zip(parsed_timestamps, parsed_timestamps[1:])
            if (current - previous).total_seconds() > session_timeout_in_seconds
        ]
        views.extend((session_start.strftime(format="%Y-%m-%d"), ip) for session_start in session_starts)
    return views


def _collect_unique_ips(
    *,
    asset_directories: list[pathlib.Path],
    use_encryption: bool = True,
    region_resolver: RegionResolver | None = None,
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
    region_resolver : RegionResolver, optional
        Resolves each IP address to its region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the collected set. If not
        provided, no exclusion is applied.

    Returns
    -------
    set of str
        The set of unique IP addresses found across all ``ips.txt`` files, excluding
        any IPs classified as a known cloud service or VPN.
    """
    unique_ips: set[str] = set()
    for asset_directory in asset_directories:
        full_ips_file_path = asset_directory / "ips.txt"
        if not full_ips_file_path.exists():
            continue
        ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        if region_resolver is None:
            unique_ips.update(ips)
        else:
            unique_ips.update(ip for ip in ips if not is_cloud_service_or_vpn_label(region_resolver.resolve(ip)))
    return unique_ips


def _summarize_dataset_requester_count(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    region_resolver: RegionResolver,
    use_encryption: bool = True,
) -> None:
    """
    Compute and save the unique requester count for a dataset.

    Reads all ``ips.txt`` files from the given asset directories, counts the
    number of unique IP addresses across the entire dataset (excluding known cloud
    service and VPN IPs), and writes the value to ``summary_file_path``.

    The count is not paired with any location, so it cannot single out a requester and is written with
    its true value on every update.

    Parameters
    ----------
    asset_directories : list of pathlib.Path
        Paths to the per-asset extraction directories containing ``ips.txt`` files.
    summary_file_path : pathlib.Path
        Destination file where the count (as a string) will be written.
    region_resolver : RegionResolver
        Resolves each IP address to its region/service label, used to exclude known cloud
        service IPs (e.g. GitHub, AWS, GCP, VPN) from the requester count.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted before reading.
        If ``False``, files are read as plaintext.
    """
    unique_ips = _collect_unique_ips(
        asset_directories=asset_directories, use_encryption=use_encryption, region_resolver=region_resolver
    )

    if not unique_ips:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_file_path.write_text(str(len(unique_ips)))


def generate_summaries(
    level: int = 0,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
    region_resolver: RegionResolver | None = None,
) -> None:
    """
    Generate summaries for each dataset in the extraction directory.

    There are several TSV summary files generated per outer level of the S3 bucket structure:
        - `by_day.tsv`: Summarizes the total bytes sent per day across all assets in the dataset.
        - `by_asset.tsv`: Summarizes the total bytes sent per asset in the dataset.
        - `by_region.tsv`: Summarizes the total bytes sent per region based on geolocations of the requester IPs.

    Requesters are geolocated while the summaries are generated: each IP address is checked against the
    published ranges of known cloud services and VPNs, and otherwise looked up in the local GeoLite2-City
    database, which is downloaded on first use (see ``update_geolite2_database``). No location of any
    requester is written to disk; only the aggregated by-region summaries are.

    Every summary is written with its true values, except for `by_region.tsv`. That one pairs activity with
    requester location, so it is written only when the update it carries moves more than
    ``region_disclosure_threshold`` resolved regions at once. Its totals therefore drift out of step with
    the other summaries between publications.

    Parameters
    ----------
    level : int
        The level of summaries to generate.
        Currently only level 0 is supported, which generates summaries for each dataset.
        Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
    use_encryption : bool
        If ``True`` (default), ``ips.txt`` files are decrypted when read.
        If ``False``, files are read as plaintext.
    region_disclosure_threshold : int
        Number of resolved regions an update to a `by_region.tsv` must move at once to be published.
        Default is ``REGION_DISCLOSURE_THRESHOLD`` (5).
    region_resolver : RegionResolver, optional
        Resolves each IP address to its region/service label. Defaults to an ``IpRegionResolver`` over the
        GeoLite2 database in the cache directory.
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

    owns_resolver = region_resolver is None
    if owns_resolver:
        region_resolver = IpRegionResolver(cache_directory=cache_directory)

    try:
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
                extraction_directory=extraction_directory,
                region_resolver=region_resolver,
                use_encryption=use_encryption,
                region_disclosure_threshold=region_disclosure_threshold,
            )

            all_archive_unique_ips.update(
                _collect_unique_ips(
                    asset_directories=asset_directories, use_encryption=use_encryption, region_resolver=region_resolver
                )
            )
    finally:
        if owns_resolver:
            region_resolver.close()

    if all_archive_unique_ips:
        archive_directory = summary_directory / "archive"
        archive_directory.mkdir(exist_ok=True)
        (archive_directory / "requester_count.tsv").write_text(str(len(all_archive_unique_ips)))


def _summarize_dataset(
    *,
    dataset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    extraction_directory: pathlib.Path,
    region_resolver: RegionResolver,
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> None:
    # Sessionizing decrypts ips.txt, so it is done once here and shared by all three summaries
    views_by_asset_directory = {
        asset_directory: _collect_asset_views(asset_directory=asset_directory, use_encryption=use_encryption)
        for asset_directory in asset_directories
    }

    _summarize_dataset_by_day(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_day.tsv",
        views_by_asset_directory=views_by_asset_directory,
    )
    _summarize_dataset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_asset.tsv",
        views_by_asset_directory=views_by_asset_directory,
        dataset_id=dataset_id,
        extraction_directory=extraction_directory,
    )
    _summarize_dataset_by_region(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_region.tsv",
        region_resolver=region_resolver,
        views_by_asset_directory=views_by_asset_directory,
        use_encryption=use_encryption,
        region_disclosure_threshold=region_disclosure_threshold,
    )
    _summarize_dataset_requester_count(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "requester_count.tsv",
        region_resolver=region_resolver,
        use_encryption=use_encryption,
    )


def _assemble_activity_summary(
    *,
    keys: list[str],
    bytes_sent: list[int],
    downloads: list[int],
    number_of_views_by_key: dict[str, int],
    key_column_name: str,
) -> pandas.DataFrame | None:
    """
    Aggregate per-request activity into one row per key.

    Rows come out in the order each key was first seen. The by-region summary publishes that order as it is;
    the by-day summary sorts afterwards, at its own call site, so the sort is deliberately not done here.

    ``number_of_views_by_key`` is indexed rather than queried with a default, because both callers pass a
    ``defaultdict(int)`` and a key with no views is expected to read as zero.

    Parameters
    ----------
    keys : list of str
        One key per request, such as a date or a region label.
    bytes_sent : list of int
        Bytes sent per request, positionally aligned with ``keys``.
    downloads : list of int
        Download flag per request, positionally aligned with ``keys``.
    number_of_views_by_key : dict of str to int
        Views already tallied per key.
    key_column_name : str
        Name of the first column of the assembled frame.

    Returns
    -------
    pandas.DataFrame or None
        The assembled summary, or ``None`` when there was nothing to aggregate, in which case the caller
        writes no file at all.
    """
    summarized_activity: collections.defaultdict[str, int] = collections.defaultdict(int)
    number_of_requests: collections.defaultdict[str, int] = collections.defaultdict(int)
    number_of_downloads: collections.defaultdict[str, int] = collections.defaultdict(int)
    for key, key_bytes_sent, download in zip(keys, bytes_sent, downloads):
        summarized_activity[key] += key_bytes_sent
        number_of_requests[key] += 1
        number_of_downloads[key] += download

    if len(summarized_activity) == 0:
        return None

    keys_ordered = list(summarized_activity.keys())
    return pandas.DataFrame(
        data={
            key_column_name: keys_ordered,
            "bytes_sent": list(summarized_activity.values()),
            "number_of_requests": [number_of_requests[key] for key in keys_ordered],
            "number_of_downloads": [number_of_downloads[key] for key in keys_ordered],
            "number_of_views": [number_of_views_by_key[key] for key in keys_ordered],
        }
    )


def _summarize_dataset_by_day(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
) -> None:
    all_dates = []
    all_bytes_sent = []
    all_downloads = []
    number_of_views_by_day: collections.defaultdict[str, int] = collections.defaultdict(int)
    for asset_directory in asset_directories:
        for view_date, _ in views_by_asset_directory.get(asset_directory, []):
            number_of_views_by_day[view_date] += 1

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        timestamps_file_path = asset_directory / "timestamps.txt"

        if not timestamps_file_path.exists():
            continue

        dates = [
            datetime.datetime.strptime(timestamp.strip(), TIMESTAMP_FORMAT).strftime(format="%Y-%m-%d")
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = _read_integers_from_file(bytes_sent_file_path)
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = _read_integers_from_file(download_file_path)
        all_downloads.extend(downloads)

    summary_table = _assemble_activity_summary(
        keys=all_dates,
        bytes_sent=all_bytes_sent,
        downloads=all_downloads,
        number_of_views_by_key=number_of_views_by_day,
        key_column_name="date",
    )
    if summary_table is None:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table.sort_values(by="date", inplace=True)
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dataset_by_asset(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
    dataset_id: str,
    extraction_directory: pathlib.Path,
) -> None:
    # The `asset_path` column is each asset's path relative to this, so it is passed in rather than recovered
    # by walking up from the summary file, which only worked while both trees shared a cache directory.
    extraction_base_path = extraction_directory / dataset_id

    summarized_activity_by_asset: collections.defaultdict[str, int] = collections.defaultdict(int)
    number_of_requests_by_asset: collections.defaultdict[str, int] = collections.defaultdict(int)
    number_of_downloads_by_asset: collections.defaultdict[str, int] = collections.defaultdict(int)
    number_of_views_by_asset: collections.defaultdict[str, int] = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        bytes_sent_file_path = asset_directory / "bytes_sent.txt"

        if not bytes_sent_file_path.exists():
            continue

        bytes_sent = _read_integers_from_file(bytes_sent_file_path)

        asset_path = str(asset_directory.relative_to(extraction_base_path))
        summarized_activity_by_asset[asset_path] += sum(bytes_sent)
        number_of_requests_by_asset[asset_path] += len(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = _read_integers_from_file(download_file_path)
        number_of_downloads_by_asset[asset_path] += sum(downloads)

        number_of_views_by_asset[asset_path] += len(views_by_asset_directory.get(asset_directory, []))

    if len(summarized_activity_by_asset) == 0:
        return

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
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dataset_by_region(
    *,
    asset_directories: list[pathlib.Path],
    summary_file_path: pathlib.Path,
    region_resolver: RegionResolver,
    views_by_asset_directory: dict[pathlib.Path, list[tuple[str, str]]],
    use_encryption: bool = True,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> None:
    all_regions = []
    all_bytes_sent = []
    all_downloads = []
    number_of_views_by_region: collections.defaultdict[str, int] = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # A view is made by a single requester, so it belongs to the region of that one IP
        for _, view_ip in views_by_asset_directory.get(asset_directory, []):
            number_of_views_by_region[region_resolver.resolve(view_ip)] += 1

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        full_ips_file_path = asset_directory / "ips.txt"

        if not full_ips_file_path.exists():
            continue

        full_ips = _read_ips_from_file(file_path=full_ips_file_path, use_encryption=use_encryption)
        regions = [region_resolver.resolve(ip) for ip in full_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = _read_integers_from_file(bytes_sent_file_path)
        all_bytes_sent.extend(bytes_sent)

        download_file_path = asset_directory / "download.txt"
        downloads = _read_integers_from_file(download_file_path)
        all_downloads.extend(downloads)

    summary_table = _assemble_activity_summary(
        keys=all_regions,
        bytes_sent=all_bytes_sent,
        downloads=all_downloads,
        number_of_views_by_key=number_of_views_by_region,
        key_column_name="region",
    )
    if summary_table is None:
        return

    _write_summary_by_region(
        summary_table=summary_table,
        summary_file_path=summary_file_path,
        region_disclosure_threshold=region_disclosure_threshold,
    )

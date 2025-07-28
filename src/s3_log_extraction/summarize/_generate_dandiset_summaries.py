import collections
import concurrent.futures
import datetime
import pathlib

import dandi.dandiapi
import pandas
import pydantic
import tqdm
import yaml

from .._parallel._utils import _handle_max_workers
from ..config import get_cache_directory, get_extraction_directory, get_summary_directory
from ..ip_utils import load_ip_cache


@pydantic.validate_call
def generate_dandiset_summaries(
    *,
    summary_directory: str | pathlib.Path | None = None,
    pick: list[str] | None = None,
    skip: list[str] | None = None,
    workers: int = -2,
) -> None:
    """
    Generate top-level summaries of access activity for all Dandisets.

    Parameters
    ----------
    summary_directory : pathlib.Path
        Path to the folder that will contain all Dandiset summaries of the S3 access logs.
    workers : int
        Number of workers to use for parallel processing.
        If -1, use all available cores. If -2, use all cores minus one.
        If 1, run in serial mode.
        Default is -2.
    pick : list of strings, optional
        A list of Dandiset IDs to exclusively select when generating summaries.
    skip : list of strings, optional
        A list of Dandiset IDs to exclude when generating summaries.
    """
    summary_directory = pathlib.Path(summary_directory) if summary_directory is not None else get_summary_directory()
    if pick is not None and skip is not None:
        message = "Cannot specify both `pick` and `skip` parameters simultaneously."
        raise ValueError(message)
    max_workers = _handle_max_workers(workers=workers)

    index_to_region = load_ip_cache(cache_type="index_to_region")

    # TODO: record and only update basic DANDI stuff based on mtime or etag
    dandiset_id_to_blob_directories, blob_id_to_asset_path = _get_dandi_asset_info()

    # TODO: cache even the dandiset listing and leverage etags
    client = dandi.dandiapi.DandiAPIClient()
    if pick is None and skip is not None:
        dandiset_ids_to_exclude = {dandiset_id: True for dandiset_id in skip}
        dandiset_ids_to_summarize = [
            dandiset.identifier
            for dandiset in client.get_dandisets()
            if dandiset_ids_to_exclude.get(dandiset.identifier, False) is False
        ]
    elif pick is not None and skip is None:
        dandiset_ids_to_summarize = pick
    else:
        dandiset_ids_to_summarize = [dandiset.identifier for dandiset in client.get_dandisets()]

    if max_workers == 1:
        for dandiset_id in tqdm.tqdm(
            iterable=dandiset_ids_to_summarize,
            total=len(dandiset_ids_to_summarize),
            desc="Summarizing Dandisets",
            position=0,
            leave=True,
            mininterval=5.0,
            smoothing=0,
            unit="dandisets",
        ):
            blob_directories = dandiset_id_to_blob_directories.get(dandiset_id, [])

            _summarize_dandiset(
                dandiset_id=dandiset_id,
                blob_directories=blob_directories,
                summary_directory=summary_directory,
                index_to_region=index_to_region,
                blob_id_to_asset_path=blob_id_to_asset_path,
            )
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    _summarize_dandiset,
                    dandiset_id=dandiset_id,
                    blob_directories=dandiset_id_to_blob_directories.get(dandiset_id, []),
                    summary_directory=summary_directory,
                    index_to_region=index_to_region,
                    blob_id_to_asset_path=blob_id_to_asset_path,
                )
                for dandiset_id in dandiset_ids_to_summarize
            ]
            collections.deque(
                (
                    future.result()
                    for future in tqdm.tqdm(
                        iterable=concurrent.futures.as_completed(futures),
                        total=len(dandiset_ids_to_summarize),
                        desc="Summarizing Dandisets",
                        position=0,
                        leave=True,
                        mininterval=5.0,
                        smoothing=0,
                        unit="dandisets",
                    )
                ),
                maxlen=0,
            )

    # Special key for multiple associations
    dandiset_id = "undetermined"
    _summarize_dandiset(
        dandiset_id=dandiset_id,
        blob_directories=dandiset_id_to_blob_directories.get(dandiset_id, []),
        summary_directory=summary_directory,
        index_to_region=index_to_region,
        blob_id_to_asset_path=blob_id_to_asset_path,
    )


def _get_dandi_asset_info(
    *,
    use_cache: bool = True,
) -> tuple[dict[str, list[pathlib.Path]], dict[str, str]]:
    cache_directory = get_cache_directory()
    dandi_cache_directory = cache_directory / "dandi"
    dandi_cache_directory.mkdir(exist_ok=True)
    extraction_directory = get_extraction_directory()

    date = datetime.datetime.now().date().strftime("%Y_%m")
    monthly_dandiset_id_to_blob_directories_cache_file_path = (
        dandi_cache_directory / f"dandiset_id_to_blob_directories_{date}.yaml"
    )
    monthly_blob_id_to_asset_path_cache_file_path = dandi_cache_directory / f"blob_id_to_asset_path_{date}.yaml"
    if use_cache is True and monthly_blob_id_to_asset_path_cache_file_path.exists():
        with monthly_dandiset_id_to_blob_directories_cache_file_path.open(mode="r") as file_stream:
            yaml_content = yaml.safe_load(stream=file_stream)

        dandiset_id_to_blob_directories = {
            dandiset_id: [pathlib.Path(blob_directory) for blob_directory in blob_directories]
            for dandiset_id, blob_directories in yaml_content.items()
        }

        with monthly_blob_id_to_asset_path_cache_file_path.open(mode="r") as file_stream:
            blob_id_to_asset_path = yaml.safe_load(stream=file_stream)
    else:
        client = dandi.dandiapi.DandiAPIClient()
        dandisets = list(client.get_dandisets())

        blob_id_to_associated_asset_paths: dict[str, set[str]] = collections.defaultdict(set)
        blob_directory_to_associated_dandiset_ids: dict[str, set[str]] = collections.defaultdict(set)
        for base_dandiset in tqdm.tqdm(
            iterable=dandisets,
            total=len(dandisets),
            desc="Mapping blob IDs to Dandisets",
            unit="dandisets",
            smoothing=0,
        ):
            dandiset_id = base_dandiset.identifier

            for version in base_dandiset.get_versions():
                versioned_dandiset = client.get_dandiset(
                    dandiset_id=base_dandiset.identifier, version_id=version.identifier
                )
                for asset in versioned_dandiset.get_assets():
                    blob_id = asset.zarr if ".zarr" in pathlib.Path(asset.path).suffixes else asset.blob
                    blob_id_to_associated_asset_paths[blob_id].add(asset.path)

                    blob_directory = (
                        extraction_directory / "zarr" / blob_id
                        if ".zarr" in pathlib.Path(asset.path).suffixes
                        else extraction_directory / "blobs" / blob_id[:3] / blob_id[3:6] / blob_id
                    )
                    blob_directory_to_associated_dandiset_ids[blob_directory].add(dandiset_id)

        blob_id_to_asset_path = dict()
        dandiset_id_to_blob_directories = collections.defaultdict(list)
        for blob_directory, dandiset_ids in blob_directory_to_associated_dandiset_ids.items():
            blob_id = blob_directory.name

            # Case 1: multiple Dandisets linked to the same blob ID
            # Cannot uniquely associate activity to a particular Dandiset, so mark Dandiset ID as undetermined
            if len(dandiset_ids) > 1:
                dandiset_id_to_blob_directories["undetermined"].append(blob_directory)
                continue
            dandiset_id = next(iter(dandiset_ids))
            dandiset_id_to_blob_directories[dandiset_id].append(blob_directory)

            # Case 2: multiple assets linked to the same blob ID within a single Dandiset (including across versions)
            # Able to uniquely associate activity to a particular Dandiset
            # But may not be able to uniquely associate activity to a particular asset path
            associated_asset_paths = blob_id_to_associated_asset_paths[blob_id]
            if len(associated_asset_paths) > 1:
                continue
            blob_id_to_asset_path[blob_id] = next(iter(associated_asset_paths))

        yaml_content = {
            dandiset_id: [str(blob_directory) for blob_directory in blob_directories]
            for dandiset_id, blob_directories in dandiset_id_to_blob_directories.items()
        }
        with monthly_dandiset_id_to_blob_directories_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=yaml_content, stream=file_stream)

        with monthly_blob_id_to_asset_path_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=blob_id_to_asset_path, stream=file_stream)

    return dandiset_id_to_blob_directories, blob_id_to_asset_path


def _summarize_dandiset(
    *,
    dandiset_id: str,
    blob_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    index_to_region: dict[int, str],
    blob_id_to_asset_path: dict[str, str],
) -> None:
    _summarize_dandiset_by_day(
        blob_directories=blob_directories, summary_file_path=summary_directory / dandiset_id / "by_day.tsv"
    )
    _summarize_dandiset_by_asset(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_asset.tsv",
        blob_id_to_asset_path=blob_id_to_asset_path,
    )
    _summarize_dandiset_by_region(
        blob_directories=blob_directories,
        summary_file_path=summary_directory / dandiset_id / "by_region.tsv",
        index_to_region=index_to_region,
    )


def _summarize_dandiset_by_day(*, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path) -> None:
    all_dates = []
    all_bytes_sent = []
    for blob_directory in blob_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        timestamps_file_path = blob_directory / "timestamps.txt"
        dates = [
            _timestamp_to_date_format(timestamp=timestamp)
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_day = collections.defaultdict(int)
    for date, bytes_sent in zip(all_dates, all_bytes_sent):
        summarized_activity_by_day[date] += bytes_sent

    if len(summarized_activity_by_day) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table = pandas.DataFrame(
        data={
            "date": list(summarized_activity_by_day.keys()),
            "bytes_sent": list(summarized_activity_by_day.values()),
        }
    )
    summary_table.sort_values(by="date", inplace=True)
    summary_table.index = range(len(summary_table))
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _timestamp_to_date_format(*, timestamp: str) -> str:
    date = f"20{timestamp[:2]}-{timestamp[2:4]}-{timestamp[4:6]}"
    return date


def _summarize_dandiset_by_asset(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, blob_id_to_asset_path: dict[str, str]
) -> None:
    summarized_activity_by_asset = collections.defaultdict(int)
    for blob_directory in blob_directories:
        blob_id = blob_directory.name

        # No extracted logs found (possible asset was never accessed); skip to next asset
        if not blob_directory.exists():
            continue

        # It is possible that this blob cannot be uniquely associated with an asset path within the Dandiset
        # (the blob ID would not be in the asset path mapping in that case)
        asset_path = blob_id_to_asset_path.get(blob_id, "undetermined")

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        summarized_activity_by_asset[asset_path] += sum(bytes_sent)

    if len(summarized_activity_by_asset) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table = pandas.DataFrame(
        data={
            "asset_path": list(summarized_activity_by_asset.keys()),
            "bytes_sent": list(summarized_activity_by_asset.values()),
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)


def _summarize_dandiset_by_region(
    *, blob_directories: list[pathlib.Path], summary_file_path: pathlib.Path, index_to_region: dict[int, str]
) -> None:
    all_regions = []
    all_bytes_sent = []
    for blob_directory in blob_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not blob_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        indexed_ips_file_path = blob_directory / "indexed_ips.txt"
        indexed_ips = [int(ip_index.strip()) for ip_index in indexed_ips_file_path.read_text().splitlines()]
        regions = [index_to_region.get(ip_index, "unknown") for ip_index in indexed_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = blob_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_region = collections.defaultdict(int)
    for region, bytes_sent in zip(all_regions, all_bytes_sent):
        summarized_activity_by_region[region] += bytes_sent

    if len(summarized_activity_by_region) == 0:
        return

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table = pandas.DataFrame(
        data={
            "region": list(summarized_activity_by_region.keys()),
            "bytes_sent": list(summarized_activity_by_region.values()),
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

import collections
import datetime
import pathlib

import dandi.dandiapi
import pandas
import tqdm
import yaml

from ..config import get_cache_directory, get_extraction_directory, get_summary_directory
from ..ip_utils import load_ip_cache


def generate_all_dandiset_summaries() -> None:
    client = dandi.dandiapi.DandiAPIClient()

    summary_directory = get_summary_directory()
    index_to_region = load_ip_cache(cache_type="index_to_region")

    # TODO: record and only update basic DANDI stuff based on mtime or etag
    dandiset_id_to_asset_directories, asset_id_to_asset_path = (
        _get_dandiset_id_to_asset_directories_and_asset_id_to_asset_path(client=client)
    )

    dandisets = list(client.get_dandisets())
    for dandiset in tqdm.tqdm(
        iterable=dandisets,
        total=len(dandisets),
        desc="Summarizing Dandisets",
        position=0,
        leave=True,
        mininterval=5.0,
        smoothing=0,
        unit="dandiset",
    ):
        dandiset_id = dandiset.identifier
        asset_directories = dandiset_id_to_asset_directories.get(dandiset_id, [])

        _summarize_dandiset(
            dandiset_id=dandiset_id,
            asset_directories=asset_directories,
            summary_directory=summary_directory,
            index_to_region=index_to_region,
            asset_id_to_asset_path=asset_id_to_asset_path,
        )

    # Special key for multiple associations
    dandiset_id = "undetermined"
    _summarize_dandiset(
        dandiset_id=dandiset_id,
        asset_directories=dandiset_id_to_asset_directories.get(dandiset_id, []),
        summary_directory=summary_directory,
        index_to_region=index_to_region,
        asset_id_to_asset_path=asset_id_to_asset_path,
    )


def _get_dandiset_id_to_asset_directories_and_asset_id_to_asset_path(
    *,
    client: dandi.dandiapi.DandiAPIClient,
    use_cache: bool = True,
) -> tuple[dict[str, list[pathlib.Path]], dict[str, str]]:
    cache_directory = get_cache_directory()
    dandi_cache_directory = cache_directory / "dandi"
    dandi_cache_directory.mkdir(exist_ok=True)
    extraction_directory = get_extraction_directory()

    date = datetime.datetime.now().date().strftime("%Y_%m")
    monthly_dandiset_id_to_asset_directories_cache_file_path = (
        dandi_cache_directory / f"dandiset_id_to_asset_directories_{date}.yaml"
    )
    monthly_asset_id_to_asset_path_cache_file_path = dandi_cache_directory / f"asset_id_to_asset_path_{date}.yaml"
    if use_cache is True and monthly_asset_id_to_asset_path_cache_file_path.exists():
        with monthly_dandiset_id_to_asset_directories_cache_file_path.open(mode="r") as file_stream:
            yaml_content = yaml.safe_load(stream=file_stream)

        dandiset_id_to_asset_directories = {
            dandiset_id: [pathlib.Path(asset_directory) for asset_directory in asset_directories]
            for dandiset_id, asset_directories in yaml_content.items()
        }

        with monthly_asset_id_to_asset_path_cache_file_path.open(mode="r") as file_stream:
            asset_id_to_asset_path = yaml.safe_load(stream=file_stream)
    else:
        asset_id_to_asset = dict()
        asset_id_to_asset_path = dict()
        asset_id_to_dandiset_ids = collections.defaultdict(set)
        dandisets = list(client.get_dandisets())
        for base_dandiset in tqdm.tqdm(
            iterable=dandisets, total=len(dandisets), desc="Updating asset caches", unit="dandiset", smoothing=0
        ):
            for version in base_dandiset.get_versions():
                dandiset = client.get_dandiset(dandiset_id=base_dandiset.identifier, version_id=version.identifier)
                for asset in dandiset.get_assets():
                    asset_id_to_asset[asset.identifier] = asset
                    asset_id_to_asset_path[asset.identifier] = asset.path
                    asset_id_to_dandiset_ids[asset.identifier].update(dandiset.identifier)

        dandiset_id_to_asset_directories = collections.defaultdict(list)
        for asset_id, dandiset_ids in asset_id_to_dandiset_ids.items():
            asset = asset_id_to_asset[asset_id]
            asset_directory = (
                extraction_directory / "zarr" / asset.zarr
                if ".zarr" in pathlib.Path(asset.path).suffixes
                else extraction_directory / "blobs" / asset.blob[:3] / asset.blob[3:6] / asset.blob
            )
            print(dandiset_ids)
            if len(dandiset_ids) > 1:
                dandiset_id_to_asset_directories["undetermined"].append(asset_directory)
            else:
                dandiset_id = next(iter(dandiset_ids))
                dandiset_id_to_asset_directories[dandiset_id].append(asset_directory)

        yaml_content = {
            dandiset_id: [str(asset_directory) for asset_directory in asset_directories]
            for dandiset_id, asset_directories in dandiset_id_to_asset_directories.items()
        }
        with monthly_dandiset_id_to_asset_directories_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=yaml_content, stream=file_stream)

        with monthly_asset_id_to_asset_path_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=asset_id_to_asset_path, stream=file_stream)

    return dandiset_id_to_asset_directories, asset_id_to_asset_path


def _summarize_dandiset(
    *,
    dandiset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    index_to_region: dict[int, str],
    asset_id_to_asset_path: dict[str, str],
) -> None:
    _summarize_dandiset_by_day(
        asset_directories=asset_directories, summary_file_path=summary_directory / dandiset_id / "by_day.tsv"
    )
    _summarize_dandiset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dandiset_id / "by_asset.tsv",
        asset_id_to_asset_path=asset_id_to_asset_path,
    )
    _summarize_dandiset_by_region(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dandiset_id / "by_region.tsv",
        index_to_region=index_to_region,
    )


def _summarize_dandiset_by_day(*, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path) -> None:
    all_dates = []
    all_bytes_sent = []
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        timestamps_file_path = asset_directory / "timestamps.txt"
        dates = [
            datetime.datetime.strptime(str(timestamp.strip()), "%y%m%d%H%M%S").strftime(format="%Y-%m-%d")
            for timestamp in timestamps_file_path.read_text().splitlines()
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
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
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=True)


def _summarize_dandiset_by_asset(
    *, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path, asset_id_to_asset_path: dict[str, str]
) -> None:
    summarized_activity_by_asset = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        asset_id = asset_directory.name
        asset_path = asset_id_to_asset_path[asset_id]
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
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=True)


def _summarize_dandiset_by_region(
    *, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path, index_to_region: dict[int, str]
) -> None:
    all_regions = []
    all_bytes_sent = []
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        indexed_ips_file_path = asset_directory / "indexed_ips.txt"
        indexed_ips = [ip_index.strip() for ip_index in indexed_ips_file_path.read_text().splitlines()]
        regions = [index_to_region.get(ip_index.strip(), "unknown") for ip_index in indexed_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = asset_directory / "bytes_sent.txt"
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
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=True)

import collections
import datetime
import pathlib

import dandi.dandiapi
import numpy
import pandas
import tqdm

from ._get_associated_assets import _get_associated_assets
from ..config import get_cache_directory
from ..ip_utils import load_ip_cache


def generate_all_dandiset_summaries(*, summary_directory: str | pathlib.Path) -> None:
    client = dandi.dandiapi.DandiAPIClient()
    index_to_region = load_ip_cache(cache_type="index_to_region")
    extraction_directory = get_cache_directory() / "extraction"

    # TODO: record and only update basic DANDI stuff based on mtime or etag
    uniquely_associated_assets_by_dandiset_id = _get_associated_assets()

    dandisets = client.get_dandisets()
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

        _summarize_dandiset(
            dandiset_id=dandiset_id,
            assets=uniquely_associated_assets_by_dandiset_id[dandiset_id],
            summary_directory=summary_directory,
            extraction_directory=extraction_directory,
            index_to_region=index_to_region,
        )


def _summarize_dandiset(
    *,
    dandiset_id: str,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_directory: pathlib.Path,
    extraction_directory: pathlib.Path,
    index_to_region: dict[int, str],
) -> None:
    _summarize_dandiset_by_day(
        assets=assets,
        summary_file_path=summary_directory / dandiset_id / "by_day.tsv",
        extraction_directory=extraction_directory,
    )
    _summarize_dandiset_by_asset(
        assets=assets,
        summary_file_path=summary_directory / dandiset_id / "by_asset.tsv",
        extraction_directory=extraction_directory,
    )
    _summarize_dandiset_by_region(
        assets=assets,
        summary_file_path=summary_directory / dandiset_id / "by_region.tsv",
        extraction_directory=extraction_directory,
        index_to_region=index_to_region,
    )


def _summarize_dandiset_by_day(
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
):
    all_dates = []
    all_bytes_sent = []
    for asset in assets:
        asset_as_path = pathlib.Path(asset.path)
        asset_suffixes = asset_as_path.suffixes

        is_asset_zarr = ".zarr" in asset_suffixes
        if is_asset_zarr:
            blob_id = asset.zarr
            extracted_asset_directory = extraction_directory / "zarr" / f"{blob_id}.tsv"
        else:
            blob_id = asset.blob
            extracted_asset_directory = extraction_directory / "blobs" / blob_id[:3] / blob_id[3:6] / f"{blob_id}.tsv"

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not extracted_asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        timestamps_file_path = extracted_asset_directory / "timestamps.bin"
        dates = [
            datetime.datetime.strptime(str(timestamp), "%y%m%d%H%M%S").strftime(format="%Y-%m-%d")
            for timestamp in numpy.memmap(filename=timestamps_file_path, mode="r", dtype="uint64")
        ]
        all_dates.extend(dates)

        bytes_sent_file_path = extracted_asset_directory / "bytes_sent.bin"
        bytes_sent = [int(value) for value in numpy.memmap(filename=bytes_sent_file_path, mode="r", dtype="uint64")]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_day = collections.defaultdict(int)
    for date, bytes_sent in zip(all_dates, all_bytes_sent):
        summarized_activity_by_day[date] += bytes_sent

    # convert dict into pandas dataframe
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
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
):
    summarized_activity_by_asset = collections.defaultdict(int)
    for asset in assets:
        asset_as_path = pathlib.Path(asset.path)
        asset_suffixes = asset_as_path.suffixes

        is_asset_zarr = ".zarr" in asset_suffixes
        if is_asset_zarr:
            blob_id = asset.zarr
            extracted_asset_directory = extraction_directory / "zarr" / f"{blob_id}.tsv"
        else:
            blob_id = asset.blob
            extracted_asset_directory = extraction_directory / "blobs" / blob_id[:3] / blob_id[3:6] / f"{blob_id}.tsv"

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not extracted_asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        bytes_sent_file_path = extracted_asset_directory / "bytes_sent.bin"
        bytes_sent = [int(value) for value in numpy.memmap(filename=bytes_sent_file_path, mode="r", dtype="uint64")]

        summarized_activity_by_asset[asset.path] += sum(bytes_sent)

    # convert dict into pandas dataframe
    summary_table = pandas.DataFrame(
        data={
            "asset_path": list(summarized_activity_by_asset.keys()),
            "bytes_sent": list(summarized_activity_by_asset.values()),
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=True)


def _summarize_dandiset_by_region(
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
    index_to_region: dict[int, str],
):
    all_regions = []
    all_bytes_sent = []
    for asset in assets:
        asset_as_path = pathlib.Path(asset.path)
        asset_suffixes = asset_as_path.suffixes

        is_asset_zarr = ".zarr" in asset_suffixes
        if is_asset_zarr:
            blob_id = asset.zarr
            extracted_asset_directory = extraction_directory / "zarr" / f"{blob_id}.tsv"
        else:
            blob_id = asset.blob
            extracted_asset_directory = extraction_directory / "blobs" / blob_id[:3] / blob_id[3:6] / f"{blob_id}.tsv"

        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing

        if not extracted_asset_directory.exists():
            continue  # No extracted logs found (possible asset was never accessed); skip to next asset

        indexed_ips_file_path = extracted_asset_directory / "indexed_ips.bin"
        indexed_ips = numpy.memmap(filename=indexed_ips_file_path, mode="r", dtype="uint64")
        regions = [index_to_region.get(ip_index, "unknown") for ip_index in indexed_ips]
        all_regions.extend(regions)

        bytes_sent_file_path = extracted_asset_directory / "bytes_sent.bin"
        bytes_sent = [int(value) for value in numpy.memmap(filename=bytes_sent_file_path, mode="r", dtype="uint64")]
        all_bytes_sent.extend(bytes_sent)

    summarized_activity_by_region = collections.defaultdict(int)
    for region, bytes_sent in zip(all_regions, all_bytes_sent):
        summarized_activity_by_region[region] += bytes_sent

    # convert dict into pandas dataframe
    summary_table = pandas.DataFrame(
        data={
            "region": list(summarized_activity_by_region.keys()),
            "bytes_sent": list(summarized_activity_by_region.values()),
        }
    )
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=True)

import collections
import datetime
import pathlib

import pandas
import tqdm

from ..config import get_extraction_directory, get_summary_directory
from ..ip_utils import load_ip_cache


def generate_summaries(level: int = 0, cache_directory: str | pathlib.Path | None = None) -> None:
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
    """
    if level != 0:
        message = (
            "\n\nCurrently only level 0 summaries are supported."
            "Please raise an issue to request this feature: https://github.com/dandi/s3-log-extraction/issues/new\n\n"
        )
        raise NotImplementedError(message)

    extraction_directory = get_extraction_directory(cache_directory=cache_directory)
    summary_directory = get_summary_directory(cache_directory=cache_directory)
    index_to_region = load_ip_cache(cache_type="index_to_region", cache_directory=cache_directory)

    datasets = [item for item in extraction_directory.iterdir() if item.is_dir()]
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

        asset_directories = [file_path.parent for file_path in dataset.rglob(pattern="*bytes_sent.txt")]
        _summarize_dataset(
            dataset_id=dataset_id,
            asset_directories=asset_directories,
            summary_directory=summary_directory,
            index_to_region=index_to_region,
        )


def _summarize_dataset(
    *,
    dataset_id: str,
    asset_directories: list[pathlib.Path],
    summary_directory: pathlib.Path,
    index_to_region: dict[int, str],
) -> None:
    _summarize_dataset_by_day(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_day.tsv",
    )
    _summarize_dataset_by_asset(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_asset.tsv",
    )
    _summarize_dataset_by_region(
        asset_directories=asset_directories,
        summary_file_path=summary_directory / dataset_id / "by_region.tsv",
        index_to_region=index_to_region,
    )


def _summarize_dataset_by_day(*, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path) -> None:
    all_dates = []
    all_bytes_sent = []
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


def _summarize_dataset_by_asset(*, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path) -> None:
    dataset_id = summary_file_path.parent.name
    extraction_base_path = summary_file_path.parent.parent.parent / "extraction" / dataset_id  # Assumes same cache dir

    summarized_activity_by_asset = collections.defaultdict(int)
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        bytes_sent_file_path = asset_directory / "bytes_sent.txt"

        if not bytes_sent_file_path.exists():
            continue

        bytes_sent = [int(value.strip()) for value in bytes_sent_file_path.read_text().splitlines()]

        asset_path = str(asset_directory.relative_to(extraction_base_path))
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


def _summarize_dataset_by_region(
    *, asset_directories: list[pathlib.Path], summary_file_path: pathlib.Path, index_to_region: dict[int, str]
) -> None:
    all_regions = []
    all_bytes_sent = []
    for asset_directory in asset_directories:
        # TODO: Could add a step here to track which object IDs have been processed, and if encountered again
        # Just copy the file over instead of reprocessing
        indexed_ips_file_path = asset_directory / "indexed_ips.txt"

        if not indexed_ips_file_path.exists():
            continue

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
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)

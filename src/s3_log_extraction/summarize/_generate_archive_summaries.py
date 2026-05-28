import pathlib

import beartype
import natsort
import pandas

from ..config import get_cache_subdirectory


@beartype.beartype
def generate_archive_summaries(cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Generate summaries by day and region for the entire archive from the mapped S3 logs.

    Parameters
    ----------
    cache_directory : path-like, optional
        The top-level cache directory from which the summary directory is derived.
        If not provided, the default cache directory is used.
    """
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")
    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(exist_ok=True)

    # TODO: deduplicate code into common helpers across tools
    # By day
    all_dataset_summaries_by_day = [
        pandas.read_table(filepath_or_buffer=dataset_by_day_summary_file_path)
        for dataset_by_day_summary_file_path in summary_directory.rglob(pattern="by_day.tsv")
        if dataset_by_day_summary_file_path.parent.name != "archive"
    ]
    aggregated_dataset_summaries_by_day = pandas.concat(objs=all_dataset_summaries_by_day, ignore_index=True)

    pre_aggregated = aggregated_dataset_summaries_by_day.groupby(by="date", as_index=False)[
        ["bytes_sent", "number_of_requests", "number_of_downloads"]
    ].sum()
    pre_aggregated.sort_values(by="date", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_day = pre_aggregated.reindex(
        columns=("date", "bytes_sent", "number_of_requests", "number_of_downloads")
    )
    aggregated_activity_by_day = aggregated_activity_by_day.astype(
        dtype={"bytes_sent": "int64", "number_of_requests": "int64", "number_of_downloads": "int64"}
    )

    archive_summary_by_day_file_path = archive_directory / "by_day.tsv"
    aggregated_activity_by_day.to_csv(
        path_or_buf=archive_summary_by_day_file_path, mode="w", sep="\t", header=True, index=False
    )

    # By region
    all_dataset_summaries_by_region = [
        pandas.read_table(filepath_or_buffer=dataset_by_region_summary_file_path)
        for dataset_by_region_summary_file_path in summary_directory.rglob(pattern="by_region.tsv")
        if dataset_by_region_summary_file_path.parent.name != "archive"
    ]
    aggregated_dataset_summaries_by_region = pandas.concat(objs=all_dataset_summaries_by_region, ignore_index=True)

    pre_aggregated = aggregated_dataset_summaries_by_region.groupby(by="region", as_index=False)[
        ["bytes_sent", "number_of_requests", "number_of_downloads"]
    ].sum()
    pre_aggregated.sort_values(by="region", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_region = pre_aggregated.reindex(
        columns=("region", "bytes_sent", "number_of_requests", "number_of_downloads")
    )
    aggregated_activity_by_region = aggregated_activity_by_region.astype(
        dtype={"bytes_sent": "int64", "number_of_requests": "int64", "number_of_downloads": "int64"}
    )

    archive_summary_by_region_file_path = archive_directory / "by_region.tsv"
    aggregated_activity_by_region.to_csv(
        path_or_buf=archive_summary_by_region_file_path, mode="w", sep="\t", header=True, index=False
    )

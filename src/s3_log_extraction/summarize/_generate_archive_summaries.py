import pathlib

import natsort
import pandas
import pydantic

from ..config import get_summary_directory


@pydantic.validate_call
def generate_archive_summaries(
    summary_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Generate summaries by day and region for the entire archive from the mapped S3 logs.

    Parameters
    ----------
    summary_directory : pathlib.Path
        Path to the folder containing all previously generated summaries of the S3 access logs.
    """
    summary_directory = pathlib.Path(summary_directory) if summary_directory is not None else get_summary_directory()
    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(exist_ok=True)

    # TODO: deduplicate code into common helpers across tools
    # By day
    all_dandiset_summaries_by_day = [
        pandas.read_table(filepath_or_buffer=dandiset_by_day_summary_file_path)
        for dandiset_by_day_summary_file_path in summary_directory.rglob(pattern="by_day.tsv")
        if dandiset_by_day_summary_file_path.parent.name != "archive"
    ]
    aggregated_dandiset_summaries_by_day = pandas.concat(objs=all_dandiset_summaries_by_day, ignore_index=True)

    pre_aggregated = aggregated_dandiset_summaries_by_day.groupby(by="date", as_index=False)["bytes_sent"].agg(
        [list, "sum"]
    )
    pre_aggregated.rename(columns={"sum": "bytes_sent"}, inplace=True)
    pre_aggregated.sort_values(by="date", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_day = pre_aggregated.reindex(columns=("date", "bytes_sent"))

    archive_summary_by_day_file_path = archive_directory / "by_day.tsv"
    aggregated_activity_by_day.to_csv(
        path_or_buf=archive_summary_by_day_file_path, mode="w", sep="\t", header=True, index=False
    )

    # By region
    all_dandiset_summaries_by_region = [
        pandas.read_table(filepath_or_buffer=dandiset_by_region_summary_file_path)
        for dandiset_by_region_summary_file_path in summary_directory.rglob(pattern="by_region.tsv")
        if dandiset_by_region_summary_file_path.parent != "archive"
    ]
    aggregated_dandiset_summaries_by_region = pandas.concat(objs=all_dandiset_summaries_by_region, ignore_index=True)

    pre_aggregated = aggregated_dandiset_summaries_by_region.groupby(by="region", as_index=False)["bytes_sent"].agg(
        [list, "sum"]
    )
    pre_aggregated.rename(columns={"sum": "bytes_sent"}, inplace=True)
    pre_aggregated.sort_values(by="region", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_region = pre_aggregated.reindex(columns=("region", "bytes_sent"))

    archive_summary_by_region_file_path = archive_directory / "by_region.tsv"
    aggregated_activity_by_region.to_csv(
        path_or_buf=archive_summary_by_region_file_path, mode="w", sep="\t", header=True, index=False
    )

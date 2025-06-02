import pathlib

import natsort
import pandas


def generate_archive_summaries(
    mapped_s3_logs_folder_path: pathlib.Path,
) -> None:
    """
    Generate summaries by day and region for the entire archive from the mapped S3 logs.

    Parameters
    ----------
    mapped_s3_logs_folder_path : pathlib.Path
        Path to the folder containing the mapped S3 logs.
    """
    mapped_s3_logs_folder_path = pathlib.Path(mapped_s3_logs_folder_path)

    # TODO: deduplicate code into common helpers across tools
    # By day
    all_dandiset_summaries_by_day = [
        pandas.read_table(filepath_or_buffer=dandiset_by_day_summary_file_path)
        for dandiset_by_day_summary_file_path in mapped_s3_logs_folder_path.rglob(pattern="dandiset_summary_by_day.tsv")
    ]
    aggregated_dandiset_summaries_by_day = pandas.concat(objs=all_dandiset_summaries_by_day, ignore_index=True)

    pre_aggregated = aggregated_dandiset_summaries_by_day.groupby(by="date", as_index=False)["bytes_sent"].agg(
        [list, "sum"]
    )
    pre_aggregated.rename(columns={"sum": "bytes_sent"}, inplace=True)
    pre_aggregated.sort_values(by="date", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_day = pre_aggregated.reindex(columns=("date", "bytes_sent"))

    archive_summary_by_day_file_path = mapped_s3_logs_folder_path / "archive_summary_by_day.tsv"
    aggregated_activity_by_day.to_csv(
        path_or_buf=archive_summary_by_day_file_path, mode="w", sep="\t", header=True, index=False
    )

    # By region
    all_dandiset_summaries_by_region = [
        pandas.read_table(filepath_or_buffer=dandiset_by_region_summary_file_path)
        for dandiset_by_region_summary_file_path in mapped_s3_logs_folder_path.rglob(
            pattern="dandiset_summary_by_region.tsv"
        )
    ]
    aggregated_dandiset_summaries_by_region = pandas.concat(objs=all_dandiset_summaries_by_region, ignore_index=True)

    pre_aggregated = aggregated_dandiset_summaries_by_region.groupby(by="region", as_index=False)["bytes_sent"].agg(
        [list, "sum"]
    )
    pre_aggregated.rename(columns={"sum": "bytes_sent"}, inplace=True)
    pre_aggregated.sort_values(by="region", key=natsort.natsort_keygen(), inplace=True)

    aggregated_activity_by_region = pre_aggregated.reindex(columns=("region", "bytes_sent"))

    archive_summary_by_region_file_path = mapped_s3_logs_folder_path / "archive_summary_by_region.tsv"
    aggregated_activity_by_region.to_csv(
        path_or_buf=archive_summary_by_region_file_path, mode="w", sep="\t", header=True, index=False
    )

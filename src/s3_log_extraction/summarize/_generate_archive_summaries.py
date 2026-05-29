import pathlib

import beartype
import natsort
import pandas

from ..config import get_cache_subdirectory


@beartype.beartype
def generate_archive_summaries(
    cache_directory: str | pathlib.Path | None = None, asset_types_in_order: tuple[str, ...] | list[str] | None = None
) -> None:
    """
    Generate summaries by day and region for the entire archive from the mapped S3 logs.

    Parameters
    ----------
    cache_directory : path-like, optional
        The top-level cache directory from which the summary directory is derived.
        If not provided, the default cache directory is used.
    asset_types_in_order : sequence[str], optional
        Preferred output column ordering for known asset types in the archive
        ``by_asset_type_per_week.tsv`` summary.
    """
    if asset_types_in_order is None:
        asset_types_in_order = []
    else:
        asset_types_in_order = list(dict.fromkeys(asset_types_in_order))

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

    # Requester count (aggregated from dataset requester_count.tsv files)
    requester_counts = [
        summary_file_path.read_text().strip()
        for summary_file_path in summary_directory.rglob(pattern="requester_count.tsv")
        if summary_file_path.parent.name != "archive"
    ]
    if requester_counts:
        min_disclosure_thresholds = [
            int(count.removeprefix("<")) for count in requester_counts if count.startswith("<")
        ]
        if min_disclosure_thresholds:
            archive_requester_count = f"<{min(min_disclosure_thresholds)}"
        else:
            archive_requester_count = str(sum(int(count) for count in requester_counts))

        archive_requester_count_file_path = archive_directory / "requester_count.tsv"
        archive_requester_count_file_path.write_text(archive_requester_count)

    # Optional by_asset_type_per_week aggregation
    all_dataset_summaries_by_asset_type_per_week = [
        pandas.read_table(filepath_or_buffer=summary_file_path)
        for summary_file_path in summary_directory.rglob(pattern="by_asset_type_per_week.tsv")
        if summary_file_path.parent.name != "archive"
    ]
    if all_dataset_summaries_by_asset_type_per_week:
        all_summary_data = pandas.concat(objs=all_dataset_summaries_by_asset_type_per_week, ignore_index=True)
        all_summary_data.fillna(value=0, inplace=True)

        all_asset_type_columns = [
            column_name for column_name in all_summary_data.columns if column_name != "week_start"
        ]
        known_asset_type_columns = [
            column_name for column_name in asset_types_in_order if column_name in all_asset_type_columns
        ]
        additional_asset_type_columns = sorted(set(all_asset_type_columns).difference(asset_types_in_order))
        asset_type_columns = [*known_asset_type_columns, *additional_asset_type_columns]
        if asset_type_columns:
            archive_summary = (
                all_summary_data.groupby(by="week_start", as_index=False)[asset_type_columns]
                .sum()
                .reindex(columns=["week_start", *asset_type_columns])
            )
            archive_summary = archive_summary.astype(dtype={column_name: "int64" for column_name in asset_type_columns})
            archive_summary.sort_values(by="week_start", key=natsort.natsort_keygen(), inplace=True)

            archive_summary_file_path = archive_directory / "by_asset_type_per_week.tsv"
            archive_summary.to_csv(path_or_buf=archive_summary_file_path, mode="w", sep="\t", header=True, index=False)

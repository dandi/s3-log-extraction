import json
import pathlib

import pandas

from ._generate_summaries import _build_totals, _coerce_activity_columns, _count_regions_and_countries
from ..config import get_cache_subdirectory


def generate_all_dataset_totals(
    cache_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Generate top-level totals of summarized access activity for all datasets.

    Activity totals are read from the by-day summary of each dataset, which always carries true values.
    The region and country counts are read from the by-region summary, which is published only once its
    update spans enough resolved regions, so they may lag the activity totals.

    Parameters
    ----------
    cache_directory : path-like, optional
        The top-level cache directory from which the summary directory is derived.
        If not provided, the default cache directory is used.
    """
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")

    all_dataset_totals = {}
    for dandiset_id_folder_path in summary_directory.iterdir():
        if not dandiset_id_folder_path.is_dir():
            continue

        dataset_id = dandiset_id_folder_path.name
        if dataset_id == "archive":
            continue

        summary_file_path = summary_directory / dataset_id / "by_day.tsv"
        if not summary_file_path.exists():
            continue
        summary = pandas.read_table(filepath_or_buffer=summary_file_path)
        _coerce_activity_columns(summary)

        number_of_unique_regions, number_of_unique_countries = _count_regions_and_countries(
            summary_directory / dataset_id / "by_region.tsv"
        )

        requester_count_file_path = summary_directory / dataset_id / "requester_count.tsv"
        all_dataset_totals[dataset_id] = _build_totals(
            summary_table=summary,
            number_of_unique_regions=number_of_unique_regions,
            number_of_unique_countries=number_of_unique_countries,
            number_of_requesters=(
                requester_count_file_path.read_text().strip() if requester_count_file_path.exists() else 0
            ),
        )

    top_level_summary_file_path = summary_directory / "totals.json"
    with top_level_summary_file_path.open(mode="w") as file_stream:
        json.dump(obj=all_dataset_totals, fp=file_stream, indent=2, sort_keys=True)

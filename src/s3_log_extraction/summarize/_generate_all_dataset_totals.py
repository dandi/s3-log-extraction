import json
import pathlib

import pandas

from ._globals import EXCLUDED_REGION_LABELS
from ..config import get_cache_subdirectory


def generate_all_dataset_totals(
    cache_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Generate top-level totals of summarized access activity for all datasets.

    Parameters
    ----------
    cache_directory : path-like, optional
        The top-level cache directory from which the summary directory is derived.
        If not provided, the default cache directory is used.
    """
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")

    # TODO: record progress over

    all_dataset_totals = {}
    for dandiset_id_folder_path in summary_directory.iterdir():
        if not dandiset_id_folder_path.is_dir():
            continue  # TODO: use better structure for separating mapped activity from summaries
        datatset_id = dandiset_id_folder_path.name

        summary_file_path = summary_directory / datatset_id / "by_region.tsv"
        if not summary_file_path.exists():
            continue
        summary = pandas.read_table(filepath_or_buffer=summary_file_path)

        unique_countries: set[str] = set()
        for region in summary["region"]:
            if region in EXCLUDED_REGION_LABELS:
                continue

            country_code, region_name = region.split("/", 1)
            if "AWS" in country_code:
                country_code = region_name.split("-")[0].upper()

            unique_countries.add(country_code)

        number_of_unique_regions = len(summary["region"])
        number_of_unique_countries = len(unique_countries)

        requester_count_file_path = summary_directory / datatset_id / "requester_count.tsv"
        number_of_requesters: str | int = (
            requester_count_file_path.read_text().strip() if requester_count_file_path.exists() else 0
        )
        if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
            number_of_requesters = int(number_of_requesters)

        all_dataset_totals[datatset_id] = {
            "total_bytes_sent": int(summary["bytes_sent"].sum()),
            "number_of_unique_regions": number_of_unique_regions,
            "number_of_unique_countries": number_of_unique_countries,
            "total_number_of_requests": int(summary["number_of_requests"].sum()),
            "total_number_of_downloads": int(summary["number_of_downloads"].sum()),
            "number_of_requesters": number_of_requesters,
        }

    top_level_summary_file_path = summary_directory / "totals.json"
    with top_level_summary_file_path.open(mode="w") as io:
        json.dump(obj=all_dataset_totals, fp=io, indent=2)

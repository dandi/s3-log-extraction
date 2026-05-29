import json
import pathlib

import pandas

from ._generate_summaries import _round_requester_count
from ..config import get_cache_subdirectory
from ..ip_utils._globals import EXCLUDED_REGION_LABELS


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

    all_dataset_totals = {}
    for dandiset_id_folder_path in summary_directory.iterdir():
        if not dandiset_id_folder_path.is_dir():
            continue

        dataset_id = dandiset_id_folder_path.name
        if dataset_id == "archive":
            continue

        summary_file_path = summary_directory / dataset_id / "by_region.tsv"
        if not summary_file_path.exists():
            continue
        summary = pandas.read_table(filepath_or_buffer=summary_file_path)
        for column_name in ("number_of_requests", "number_of_downloads"):
            summary[column_name] = pandas.to_numeric(summary[column_name], errors="coerce").fillna(0).astype("int64")

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

        requester_count_file_path = summary_directory / dataset_id / "requester_count.tsv"
        number_of_requesters: str | int = (
            requester_count_file_path.read_text().strip() if requester_count_file_path.exists() else 0
        )
        if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
            number_of_requesters = int(number_of_requesters)

        all_dataset_totals[dataset_id] = {
            "total_bytes_sent": int(summary["bytes_sent"].sum()),
            "number_of_unique_regions": number_of_unique_regions,
            "number_of_unique_countries": number_of_unique_countries,
            "total_number_of_requests": _round_requester_count(
                count=int(summary["number_of_requests"].sum()), modulo=20, minimum=50
            ),
            "total_number_of_downloads": _round_requester_count(
                count=int(summary["number_of_downloads"].sum()), modulo=20, minimum=50
            ),
            "number_of_requesters": number_of_requesters,
        }

    top_level_summary_file_path = summary_directory / "totals.json"
    with top_level_summary_file_path.open(mode="w") as io:
        json.dump(obj=all_dataset_totals, fp=io, indent=2, sort_keys=True)

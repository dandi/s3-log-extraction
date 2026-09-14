import json
import pathlib

import beartype
import pandas

from ._utils import _build_totals, _coerce_activity_columns, _count_regions_and_countries
from ..config import get_cache_subdirectory


@beartype.beartype
def generate_archive_totals(
    cache_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Generate top-level totals of the entire archive from the archive summaries in the mapped S3 logs folder.

    Activity totals are read from the archive by-day summary, which always carries true values. The region
    and country counts are read from the archive by-region summary, which is published only once its update
    spans enough resolved regions, so they may lag the activity totals.

    Parameters
    ----------
    cache_directory : path-like, optional
        The top-level cache directory from which the summary directory is derived.
        If not provided, the default cache directory is used.
    """
    summary_directory = get_cache_subdirectory(cache_directory=cache_directory, name="summaries")
    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(exist_ok=True)

    summary_file_path = archive_directory / "by_day.tsv"
    if not summary_file_path.exists():
        message = (
            f"Archive by-day summary file not found: {summary_file_path}. "
            "Run archive summaries before archive totals."
        )
        raise FileNotFoundError(message)

    summary = pandas.read_table(filepath_or_buffer=summary_file_path)
    _coerce_activity_columns(summary)

    number_of_unique_regions, number_of_unique_countries = _count_regions_and_countries(
        archive_directory / "by_region.tsv"
    )

    requester_count_file_path = archive_directory / "requester_count.tsv"
    if not requester_count_file_path.exists():
        message = (
            f"Archive requester count file not found: {requester_count_file_path}. "
            "Run dataset summaries before archive totals; the archive requester count is deduplicated "
            "across datasets and so is written by that step, not by the archive summaries."
        )
        raise FileNotFoundError(message)

    archive_totals = _build_totals(
        summary_table=summary,
        number_of_unique_regions=number_of_unique_regions,
        number_of_unique_countries=number_of_unique_countries,
        number_of_requesters=requester_count_file_path.read_text().strip(),
    )

    archive_totals_file_path = summary_directory / "archive_totals.json"
    with archive_totals_file_path.open(mode="w") as file_stream:
        json.dump(obj=archive_totals, fp=file_stream, indent=2, sort_keys=True)

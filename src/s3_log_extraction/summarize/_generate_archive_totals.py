import json
import pathlib

import beartype
import pandas

from ._generate_summaries import _count_regions_and_countries
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
    for column_name in ("number_of_requests", "number_of_downloads", "number_of_views"):
        if column_name not in summary.columns:  # Summarized before views were reported
            summary[column_name] = 0
        summary[column_name] = pandas.to_numeric(summary[column_name], errors="coerce").fillna(0).astype("int64")

    number_of_unique_regions, number_of_unique_countries = _count_regions_and_countries(
        archive_directory / "by_region.tsv"
    )

    requester_count_file_path = archive_directory / "requester_count.tsv"
    if not requester_count_file_path.exists():
        msg = (
            f"Archive requester count file not found: {requester_count_file_path}. "
            "Run archive summaries before archive totals."
        )
        raise FileNotFoundError(msg)

    number_of_requesters: str | int = requester_count_file_path.read_text().strip()
    if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
        number_of_requesters = int(number_of_requesters)

    archive_totals = {
        "total_bytes_sent": int(summary["bytes_sent"].sum()),
        "number_of_unique_regions": number_of_unique_regions,
        "number_of_unique_countries": number_of_unique_countries,
        "total_number_of_requests": int(summary["number_of_requests"].sum()),
        "total_number_of_downloads": int(summary["number_of_downloads"].sum()),
        "number_of_requesters": number_of_requesters,
        "total_number_of_views": int(summary["number_of_views"].sum()),
    }

    archive_totals_file_path = summary_directory / "archive_totals.json"
    with archive_totals_file_path.open(mode="w") as io:
        json.dump(obj=archive_totals, fp=io, indent=2, sort_keys=True)

import json
import pathlib

import pandas
import pydantic

from ..config import get_summary_directory


@pydantic.validate_call
def generate_archive_totals(
    summary_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Generate top-level totals of the entire archive from the archive summaries in the mapped S3 logs folder.

    Parameters
    ----------
    summary_directory : pathlib.Path
        Path to the folder containing all previously generated summaries of the S3 access logs.
    """
    summary_directory = pathlib.Path(summary_directory) if summary_directory is not None else get_summary_directory()
    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(exist_ok=True)

    summary_file_path = archive_directory / "by_region.tsv"
    summary = pandas.read_table(filepath_or_buffer=summary_file_path)

    unique_countries: set[str] = set()
    for region in summary["region"]:
        if region in ["VPN", "GitHub", "unknown"]:
            continue

        region_split = region.split("/")
        country_code = region_split[0]
        region_code = "-".join(region_split[1:])
        if "AWS" in country_code:
            country_code = region_code.split("-")[0].upper()

        unique_countries.add(country_code)

    number_of_unique_regions = len(summary["region"])
    number_of_unique_countries = len(unique_countries)

    requester_count_file_path = archive_directory / "requester_count.tsv"
    number_of_requesters: str | int = (
        requester_count_file_path.read_text().strip() if requester_count_file_path.exists() else 0
    )
    if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
        number_of_requesters = int(number_of_requesters)

    archive_totals = {
        "total_bytes_sent": int(summary["bytes_sent"].sum()),
        "number_of_unique_regions": number_of_unique_regions,
        "number_of_unique_countries": number_of_unique_countries,
        "total_number_of_requests": int(summary["number_of_requests"].sum()),
        "total_number_of_downloads": int(summary["number_of_downloads"].sum()),
        "number_of_requesters": number_of_requesters,
    }

    archive_totals_file_path = summary_directory / "archive_totals.json"
    with archive_totals_file_path.open(mode="w") as io:
        json.dump(obj=archive_totals, fp=io, indent=2)

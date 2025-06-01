import json
import pathlib

import pandas


def generate_archive_totals(
    mapped_s3_logs_folder_path: str | pathlib.Path,
) -> None:
    """
    Generate top-level totals of the entire archive from the archive summaries in the mapped S3 logs folder.

    Parameters
    ----------
    mapped_s3_logs_folder_path : pathlib.Path
        Path to the folder containing the mapped S3 logs.
    """
    mapped_s3_logs_folder_path = pathlib.Path(mapped_s3_logs_folder_path)

    summary_file_path = mapped_s3_logs_folder_path / "archive_summary_by_region.tsv"
    summary = pandas.read_table(filepath_or_buffer=summary_file_path)

    unique_countries = {}
    for region in summary["region"]:
        if region in ["VPN", "GitHub", "unknown"]:
            continue

        country_code, region_name = region.split("/")
        if "AWS" in country_code:
            country_code = region_name.split("-")[0].upper()

        unique_countries[country_code] = True

    number_of_unique_regions = len(summary["region"])
    number_of_unique_countries = len(unique_countries)
    archive_totals = {
        "total_bytes_sent": int(summary["bytes_sent"].sum()),
        "number_of_unique_regions": number_of_unique_regions,
        "number_of_unique_countries": number_of_unique_countries,
    }

    archive_totals_file_path = mapped_s3_logs_folder_path / "archive_totals.json"
    with archive_totals_file_path.open(mode="w") as io:
        json.dump(obj=archive_totals, fp=io)

import json
import pathlib

import pandas


def generate_all_dandiset_totals(
    summary_directory: str | pathlib.Path,
) -> None:
    """
    Generate top-level totals of summarized access activity for all dandisets.

    Parameters
    ----------
    summary_directory : pathlib.Path
        Path to the folder containing all Dandiset summaries of the S3 access logs.
    """
    summary_directory = pathlib.Path(summary_directory)

    # TODO: record progress over

    all_dandiset_totals = {}
    for dandiset_id_folder_path in summary_directory.iterdir():
        if not dandiset_id_folder_path.is_dir():
            continue  # TODO: use better structure for separating mapped activity from summaries
        dandiset_id = dandiset_id_folder_path.name

        summary_file_path = summary_directory / dandiset_id / "dandiset_summary_by_region.tsv"
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
        all_dandiset_totals[dandiset_id] = {
            "total_bytes_sent": int(summary["bytes_sent"].sum()),
            "number_of_unique_regions": number_of_unique_regions,
            "number_of_unique_countries": number_of_unique_countries,
        }

    top_level_summary_file_path = summary_directory / "all_dandiset_totals.json"
    with top_level_summary_file_path.open(mode="w") as io:
        json.dump(obj=all_dandiset_totals, fp=io)

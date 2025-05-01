import json
import pathlib

import pandas


def generate_all_dandiset_totals(
    mapped_s3_logs_folder_path: pathlib.Path,
) -> None:
    """
    Generate top-level totals of the summaries for all dandisets from the mapped S3 logs.

    Parameters
    ----------
    mapped_s3_logs_folder_path : pathlib.Path
        Path to the folder containing the mapped S3 logs.
    """
    mapped_s3_logs_folder_path = pathlib.Path(mapped_s3_logs_folder_path)

    all_dandiset_totals = {}
    for dandiset_id in mapped_s3_logs_folder_path.iterdir():
        summary_file_path = mapped_s3_logs_folder_path / dandiset_id / "dandiset_summary_by_region.tsv"

        summary = pandas.read_table(filepath_or_buffer=summary_file_path, index_col=0)

        unique_countries = {}
        for region in summary["region"]:
            country_code, region_name = region.split("/")

            if "AWS" in country_code:
                country_code = region_name.split("-")[0].upper()

            unique_countries[country_code] = True

        number_of_unique_regions = len(summary["region"])
        number_of_unique_countries = len(unique_countries)
        all_dandiset_totals[dandiset_id] = {
            "total_bytes_sent": summary["bytes_sent"].sum(),
            "number_of_unique_regions": number_of_unique_regions,
            "number_of_unique_countries": number_of_unique_countries,
        }

    top_level_summary_file_path = mapped_s3_logs_folder_path / "all_dandiset_totals.json"
    with top_level_summary_file_path.open(mode="w") as io:
        json.dump(obj=all_dandiset_totals, fp=io)

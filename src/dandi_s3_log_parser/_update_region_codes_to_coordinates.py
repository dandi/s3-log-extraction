import json
import os
import pathlib
import time

import pandas
import requests

from ._globals import _DEFAULT_REGION_CODES_TO_COORDINATES


def update_region_codes_to_coordinates(mapped_s3_logs_folder_path: str | pathlib.Path) -> None:
    """
    Update the `region_codes_to_coordinates.json` file in the cache directory.

    Parameters
    ----------
    mapped_s3_logs_folder_path : pathlib.Path
        Path to the folder containing the mapped S3 logs.
    """
    opencage_api_key = os.environ.get("OPENCAGE_API_KEY", None)
    if opencage_api_key is None:
        message = "`OPENCAGE_API_KEY` environment variable is not set."
        raise ValueError(message)

    mapped_s3_logs_folder_path = pathlib.Path(mapped_s3_logs_folder_path)

    archive_summary_by_region_file_path = mapped_s3_logs_folder_path / "archive_summary_by_region.tsv"
    archive_summary_by_region = pandas.read_table(filepath_or_buffer=archive_summary_by_region_file_path)

    cache_directory = pathlib.Path.home() / ".cache"
    cache_directory.mkdir(exist_ok=True)

    log_cache_directory = cache_directory / "dandi_s3_log_parser"
    log_cache_directory.mkdir(exist_ok=True)

    region_codes_to_coordinates: dict[str, dict[str, float]] = _DEFAULT_REGION_CODES_TO_COORDINATES
    region_codes_to_coordinates_file_path = log_cache_directory / "region_codes_to_coordinates.json"
    if region_codes_to_coordinates_file_path.exists():
        with region_codes_to_coordinates_file_path.open(mode="r") as io:
            previous_region_codes_to_coordinates = json.load(io)
            region_codes_to_coordinates.update(previous_region_codes_to_coordinates)

    for _, row in archive_summary_by_region.iterrows():
        region_code = row["region"]
        if region_codes_to_coordinates.get(region_code, None) is None:
            # TODO: look into batch processing or async requests here
            coordinates = _get_coordinates_from_opencage(region_code=region_code, opencage_api_key=opencage_api_key)
            region_codes_to_coordinates[region_code] = coordinates

            print(f"Retrieved coordinates for {region_code}: {coordinates}")  # TODO: just testing
            time.sleep(5)  # TODO: just testing

    with region_codes_to_coordinates_file_path.open(mode="w") as io:
        json.dump(obj=region_codes_to_coordinates, fp=io)


def _get_coordinates_from_opencage(region_code: str, opencage_api_key: str) -> dict[str, float]:
    """
    Use the OpenCage API to get the coordinates (in decimal degrees form) for a ISO 3166 country/region code.

    Note that multiple results might be returned by the query, and some may not correctly correspond to the country.
    Also note that the order of latitude and longitude are reversed in the response, which is corrected in this output.
    """
    response = requests.get(
        url=f"https://api.opencagedata.com/geocode/v1/geojson?q={region_code}&key={opencage_api_key}"
    )

    # TODO: add retries logic, more robust code handling, etc.?
    if response.status_code != 200:
        message = f"Failed to fetch coordinates for region code: {region_code}"
        raise ValueError(message)

    info = response.json()
    features = info["features"]

    country_code = region_code.split("/")[0].lower()
    matching_features = [
        feature for feature in features if feature["properties"]["components"]["country_code"] == country_code
    ]
    number_of_matches = len(matching_features)

    if number_of_matches == 0:
        message = f"Could not find a match for region code: {region_code}"
        raise ValueError(message)

    if number_of_matches > 1:
        message = (
            f"\nMultiple matching features found for region code: {region_code}\n\n"
            f"{json.dumps(matching_features, indent=2)}\n"
        )
        raise ValueError(message)

    matching_feature = matching_features[0]["geometry"]["coordinates"]
    latitude = matching_feature["geometry"]["coordinates"][
        1
    ]  # Remember to use correct order for latitude and longitude
    longitude = matching_feature["geometry"]["coordinates"][0]
    coordinates = {"latitude": latitude, "longitude": longitude}

    return coordinates

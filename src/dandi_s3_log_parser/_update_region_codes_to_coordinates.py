import functools
import json
import os
import pathlib
import typing

import ipinfo
import pandas
import requests

from ._globals import _DEFAULT_REGION_CODES_TO_COORDINATES, _KNOWN_SERVICES
from ._ip_utils import _get_cidr_address_ranges_and_subregions


def update_region_codes_to_coordinates(
    *, mapped_s3_logs_folder_path: str | pathlib.Path, cache_directory: str | pathlib.Path | None = None
) -> None:
    """
    Update the `region_codes_to_coordinates.json` file in the cache directory.

    Parameters
    ----------
    mapped_s3_logs_folder_path : path-like
        Path to the folder containing the mapped S3 logs.
    cache_directory : path-like, optional
        Path to the directory where the cache files will be stored.
        Defaults to `~/.cache/dandi_s3_log_parser/`.
    """
    opencage_api_key = os.environ.get("OPENCAGE_API_KEY", None)
    ipinfo_api_key = os.environ.get("IPINFO_CREDENTIALS", None)

    api_keys = {"OPENCAGE_API_KEY": opencage_api_key, "IPINFO_CREDENTIALS": ipinfo_api_key}
    for environment_variable_name, api_key in api_keys.items():
        if api_key is None:
            message = f"`{environment_variable_name}` environment variable is not set."
            raise ValueError(message)

    mapped_s3_logs_folder_path = pathlib.Path(mapped_s3_logs_folder_path)
    cache_directory = pathlib.Path(cache_directory) if cache_directory is not None else pathlib.Path.home() / ".cache"
    cache_directory.mkdir(exist_ok=True)
    log_parser_cache_directory = cache_directory / "dandi_s3_log_parser"
    log_parser_cache_directory.mkdir(exist_ok=True)

    archive_summary_by_region_file_path = mapped_s3_logs_folder_path / "archive_summary_by_region.tsv"
    archive_summary_by_region = pandas.read_table(filepath_or_buffer=archive_summary_by_region_file_path)

    region_codes_to_coordinates: dict[str, dict[str, float]] = _DEFAULT_REGION_CODES_TO_COORDINATES
    region_codes_to_coordinates_file_path = log_parser_cache_directory / "region_codes_to_coordinates.json"
    if region_codes_to_coordinates_file_path.exists():
        with region_codes_to_coordinates_file_path.open(mode="r") as io:
            previous_region_codes_to_coordinates = json.load(io)
            region_codes_to_coordinates.update(previous_region_codes_to_coordinates)

    for _, row in archive_summary_by_region.iterrows():
        region_code = row["region"]
        if region_codes_to_coordinates.get(region_code, None) is None:
            # TODO: look into batch processing or async requests here
            coordinates = _get_coordinates(
                region_code=region_code,
                opencage_api_key=opencage_api_key,
                ipinfo_api_key=ipinfo_api_key,
                log_parser_cache_directory=log_parser_cache_directory,
            )
            region_codes_to_coordinates[region_code] = coordinates

    with region_codes_to_coordinates_file_path.open(mode="w") as io:
        json.dump(obj=region_codes_to_coordinates, fp=io)


def _get_coordinates(
    *, region_code: str, opencage_api_key: str, ipinfo_api_key: str, log_parser_cache_directory: pathlib.Path
) -> dict[str, float]:
    """
    Get the coordinates for a region code.

    May be from either a cloud region (e.g., "AWS/us-east-1") or a country/region code (e.g., "US/California").

    Parameters
    ----------
    region_code : str
        The region code to get the coordinates for.
    opencage_api_key : str
        The OpenCage API key.
    ipinfo_api_key : str
        The IPinfo API key.
    log_parser_cache_directory : pathlib.Path
        The directory to store the cache files in.

    Returns
    -------
    dict[str, float]
        A dictionary containing the latitude and longitude of the region code.
    """
    country_code = region_code.split("/")[0]
    if country_code in _KNOWN_SERVICES:
        coordinates = _get_service_coordinates_from_ipinfo(
            region_code=region_code,
            ipinfo_api_key=ipinfo_api_key,
            log_parser_cache_directory=log_parser_cache_directory,
        )
    else:
        coordinates = _get_coordinates_from_opencage(region_code=region_code, opencage_api_key=opencage_api_key)

    return coordinates


def _get_service_coordinates_from_ipinfo(
    *, region_code: str, ipinfo_api_key: str, log_parser_cache_directory: pathlib.Path
) -> dict[str, float]:
    # Note that services with a single code (e.g., "GitHub") should be handled via the global default dictionary
    service_name, subregion = region_code.split("/")

    service_coordinates = _retrieve_service_coordinates_cache(log_parser_cache_directory=log_parser_cache_directory)

    coordinates = service_coordinates.get(service_name, None)
    if coordinates is not None:
        return coordinates

    cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)
    subregion_to_cidr_address = {subregion: cidr_address for cidr_address, subregion in cidr_addresses_and_subregions}

    handler = ipinfo.getHandler(access_token=ipinfo_api_key)

    ip_address = subregion_to_cidr_address[subregion].split("/")[0]
    details = handler.getDetails(ip_address=ip_address)
    latitude = details["latitude"]
    longitude = details["longitude"]
    coordinates = {"latitude": latitude, "longitude": longitude}

    service_coordinates[region_code] = coordinates
    _save_service_coordinates_cache(
        service_coordinates=service_coordinates, log_cache_directory=log_parser_cache_directory
    )

    return coordinates


@functools.lru_cache
def _retrieve_service_coordinates_cache(*, log_parser_cache_directory: pathlib.Path) -> dict[str, dict[str, float]]:
    service_coordinates_file_path = log_parser_cache_directory / "service_coordinates.json"
    if not service_coordinates_file_path.exists():
        return {}

    with service_coordinates_file_path.open(mode="r") as io:
        service_coordinates = json.load(io)

    return service_coordinates


def _save_service_coordinates_cache(
    *, service_coordinates: dict[str, dict[str, float]], log_cache_directory: pathlib.Path
) -> None:
    service_coordinates_file_path = log_cache_directory / "service_coordinates.json"
    with service_coordinates_file_path.open(mode="w") as io:
        json.dump(obj=service_coordinates, fp=io)


def _get_coordinates_from_opencage(*, region_code: str, opencage_api_key: str) -> dict[str, float]:
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

    match number_of_matches:
        case 0:
            message = f"Could not find a match for region code: {region_code}"
            raise ValueError(message)
        case 1:
            matching_feature = matching_features[0]
        case 2:
            # Common situation is that a name is both the same as its city and the region that city is in
            # E.g., Buenos Aires, Buenos Aires, AR

            feature_has_city: dict[str, dict[str, typing.Any]] = {
                feature: feature["properties"]["components"].get("city", None) is not None
                for feature in matching_features
            }
            if all(feature_has_city):
                message = (
                    f"\nMultiple matching features found for region code: {region_code}\n\n"
                    f"{json.dumps(matching_features, indent=2)}\n"
                )
                raise ValueError(message)

            matching_feature = next(feature for feature, has_city in feature_has_city.items() if has_city is True)
        case _:
            message = (
                f"\nMultiple matching features found for region code: {region_code}\n\n"
                f"{json.dumps(matching_features, indent=2)}\n"
            )
            raise ValueError(message)

    latitude = matching_feature["geometry"]["coordinates"][1]  # Remember to use corrected order latitude and longitude
    longitude = matching_feature["geometry"]["coordinates"][0]
    coordinates = {"latitude": latitude, "longitude": longitude}

    return coordinates

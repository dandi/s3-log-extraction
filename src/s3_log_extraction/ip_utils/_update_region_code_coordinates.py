import os
import pathlib

import natsort
import tqdm
import yaml

from ._globals import _DEFAULT_REGION_CODES_TO_COORDINATES, _KNOWN_SERVICES
from ._ip_cache import get_ip_cache_directory, load_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions


def update_region_code_coordinates(
    cache_directory: str | pathlib.Path | None = None,
) -> None:
    """
    Update the `region_codes_to_coordinates.yaml` file in the cache directory.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.
    """
    import ipinfo
    import opencage.geocoder

    opencage_api_key = os.environ.get("OPENCAGE_API_KEY", None)
    ipinfo_api_key = os.environ.get("IPINFO_API_KEY", None)

    api_keys = {"OPENCAGE_API_KEY": opencage_api_key, "IPINFO_API_KEY": ipinfo_api_key}
    for environment_variable_name, api_key in api_keys.items():
        if api_key is None:
            message = f"`{environment_variable_name}` environment variable is not set."
            raise ValueError(message)
    ipinfo_client = ipinfo.getHandler(access_token=ipinfo_api_key)
    opencage_client = opencage.geocoder.OpenCageGeocode(key=opencage_api_key)

    ip_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)

    index_to_region_codes_file_path = ip_cache_directory / "index_to_region.yaml"
    if not index_to_region_codes_file_path.exists():
        message = (
            f"\nCannot update region codes to coordinates because the indexed regions file does not exist: "
            f"{index_to_region_codes_file_path}\n\n"
            f"Please run `s3_log_extractor.update_index_to_region_codes()` first to create the indexed regions file.\n"
        )
        raise FileNotFoundError(message)

    service_coordinates_file_path = ip_cache_directory / "service_coordinates.yaml"
    if not service_coordinates_file_path.exists():
        service_coordinates_file_path.touch()
    with service_coordinates_file_path.open(mode="r") as file_stream:
        service_coordinates = yaml.safe_load(stream=file_stream) or {}

    region_codes_to_coordinates: dict[str, dict[str, float]] = _DEFAULT_REGION_CODES_TO_COORDINATES
    previous_region_codes_to_coordinates = load_ip_cache(
        cache_type="region_codes_to_coordinates", cache_directory=cache_directory
    )
    region_codes_to_coordinates.update(previous_region_codes_to_coordinates)

    indexed_region_codes = load_ip_cache(cache_type="index_to_region", cache_directory=cache_directory)
    region_codes_to_update = set(indexed_region_codes.values()) - set(region_codes_to_coordinates.keys())
    opencage_failures = []
    for country_and_region_code in tqdm.tqdm(
        iterable=region_codes_to_update,
        total=len(region_codes_to_update),
        desc="Updating region coordinates",
        smoothing=0,
        unit="regions",
    ):
        # Bogon IPs do not have coordinates, so skip
        if country_and_region_code == "bogon":
            continue

        coordinates = _get_coordinates_from_region_code(
            country_and_region_code=country_and_region_code,
            ipinfo_client=ipinfo_client,
            opencage_client=opencage_client,
            service_coordinates=service_coordinates,
            opencage_failures=opencage_failures,
        )

        if coordinates is not None:
            region_codes_to_coordinates[country_and_region_code] = coordinates

    region_codes_to_coordinates_ordered = {
        key: region_codes_to_coordinates[key] for key in natsort.natsorted(seq=region_codes_to_coordinates.keys())
    }

    region_codes_to_coordinates_file_path = ip_cache_directory / "region_codes_to_coordinates.yaml"
    with region_codes_to_coordinates_file_path.open(mode="w") as file_stream:
        yaml.dump(data=region_codes_to_coordinates_ordered, stream=file_stream)
    with service_coordinates_file_path.open(mode="w") as file_stream:
        yaml.dump(data=service_coordinates, stream=file_stream)

    if any(opencage_failures):
        message = (
            f"\nThe following region codes could not be resolved using the OpenCage API:\n"
            f"{', '.join(opencage_failures)}\n\n"
        )
        print(message)


def _get_coordinates_from_region_code(
    *,
    country_and_region_code: str,
    ipinfo_client: "ipinfo.Handler",
    opencage_client: "opencage.geocoder.OpenCageGeocode",
    service_coordinates: dict[str, dict[str, float]],
    opencage_failures: list[str],
) -> dict[str, float]:
    """
    Get the coordinates for a region code.

    May be from either a cloud region (e.g., "AWS/us-east-1") or a country/region code (e.g., "US/California").

    Parameters
    ----------
    country_and_region_code : str
        The region code to get the coordinates for.
    ipinfo_client : ipinfo.Handler
        The IPInfo handler to use for fetching coordinates.
    opencage_api_key : str
        The OpenCage API key.
    service_coordinates : dict[str, dict[str, float]]
        A dictionary containing the coordinates of known services.

    Returns
    -------
    dict[str, float]
        A dictionary containing the latitude and longitude of the region code.
    """
    country_code = country_and_region_code.split("/")[0]
    if country_code in _KNOWN_SERVICES:
        coordinates = _get_service_coordinates_from_ipinfo(
            country_and_region_code=country_and_region_code,
            ipinfo_client=ipinfo_client,
            service_coordinates=service_coordinates,
        )
    else:
        coordinates = _get_coordinates_from_opencage(
            country_and_region_code=country_and_region_code,
            opencage_client=opencage_client,
            opencage_failures=opencage_failures,
        )

    return coordinates


def _get_service_coordinates_from_ipinfo(
    *,
    country_and_region_code: str,
    ipinfo_client: "ipinfo.Handler",
    service_coordinates: dict[str, dict[str, float]],
) -> dict[str, float]:
    # Note that services with a single code (e.g., "GitHub") should be handled via the global default dictionary
    service_name, subregion = country_and_region_code.split("/")

    coordinates = service_coordinates.get(service_name, None)
    if coordinates is not None:
        return coordinates

    cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)
    subregion_to_cidr_address = {subregion: cidr_address for cidr_address, subregion in cidr_addresses_and_subregions}

    ip_address = subregion_to_cidr_address[subregion].split("/")[0]
    details = ipinfo_client.getDetails(ip_address=ip_address).details
    latitude = details["latitude"]
    longitude = details["longitude"]
    coordinates = {"latitude": latitude, "longitude": longitude}

    service_coordinates[country_and_region_code] = coordinates

    return coordinates


def _get_coordinates_from_opencage(
    *, country_and_region_code: str, opencage_client: "opencage.geocoder.OpenCageGeocode", opencage_failures: list[str]
) -> dict[str, float]:
    """
    Use the OpenCage API to get the coordinates (in decimal degrees form) for a ISO 3166 country/region code.

    Note that multiple results might be returned by the query, and some may not correctly correspond to the country.
    Also note that the order of latitude and longitude are reversed in the response, which is corrected in this output.
    """
    results = opencage_client.geocode(country_and_region_code)

    if not any(results):
        opencage_failures.append(country_and_region_code)
        return

    latitude = results[0]["geometry"]["lat"]
    longitude = results[0]["geometry"]["lng"]
    coordinates = {"latitude": latitude, "longitude": longitude}

    return coordinates

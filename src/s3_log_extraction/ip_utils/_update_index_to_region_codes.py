import ipaddress
import os

import ipinfo
import yaml

from ._globals import _KNOWN_SERVICES
from ._ip_cache import get_ip_cache_directory, load_index_to_ip, load_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions


def update_index_to_region_codes() -> str | None:
    """Update the `indexed_region_codes.yaml` file in the cache directory."""
    ipinfo_api_key = os.environ.get("IPINFO_API_KEY", None)
    if ipinfo_api_key is None:
        message = "The environment variable 'IPINFO_API_KEY' must be set to import `s3_log_extraction`!"
        raise ValueError(message)  # pragma: no cover
    ipinfo_handler = ipinfo.getHandler(access_token=ipinfo_api_key)

    ip_cache_directory = get_ip_cache_directory()
    index_not_in_services = load_ip_cache(cache_type="index_not_in_services")

    index_to_ip = load_index_to_ip()
    index_to_region = load_ip_cache(cache_type="index_to_region")
    indices_to_update = set(index_to_ip.keys()) - set(index_to_region.keys())
    _update_region_codes_from_ip_indices(
        ip_indices=indices_to_update,
        index_to_ip=index_to_ip,
        index_to_region=index_to_region,
        index_not_in_services=index_not_in_services,
        ipinfo_handler=ipinfo_handler,
    )

    indexed_regions_file_path = ip_cache_directory / "index_to_region.yaml"
    with indexed_regions_file_path.open(mode="w") as file_stream:
        yaml.dump(data=index_to_region, stream=file_stream)


def _update_region_codes_from_ip_indices(
    *,
    ip_indices: int,
    index_to_ip: dict[int, str],
    index_to_region: dict[int, str],
    index_not_in_services: dict[int, bool],
    ipinfo_handler: ipinfo.Handler,
) -> dict[int, str]:
    # Determine if IP address belongs to GitHub, AWS, Google, or known VPNs
    # Azure not yet easily doable; keep an eye on
    # https://learn.microsoft.com/en-us/answers/questions/1410071/up-to-date-azure-public-api-to-get-azure-ip-ranges
    # maybe it will change in the future
    for ip_index in ip_indices:
        if index_not_in_services.get(ip_index, None) is None:
            for service_name in _KNOWN_SERVICES:
                cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)

                ip_address = index_to_ip[ip_index]
                matched_cidr_address_and_subregion = next(
                    (
                        (cidr_address, subregion)
                        for cidr_address, subregion in cidr_addresses_and_subregions
                        if ipaddress.ip_address(address=ip_address) in ipaddress.ip_network(address=cidr_address)
                    ),
                    None,
                )
                if matched_cidr_address_and_subregion is not None:
                    region_service_string = service_name

                    subregion = matched_cidr_address_and_subregion[1]
                    if subregion is not None:
                        region_service_string += f"/{subregion}"
                    index_to_region[ip_index] = region_service_string
        index_not_in_services[ip_index] = True

    # Lines cannot be covered without testing on a real IP
    # try:  # pragma: no cover
    ip_address_to_ip_index_not_in_services = {
        index_to_ip[ip_index]: ip_index
        for ip_index in ip_indices
        if index_not_in_services.get(ip_index, False) is False
    }
    all_details = ipinfo_handler.getBatchDetails(ip_addresses=list(ip_address_to_ip_index_not_in_services.keys()))
    for ip_address, details in all_details:
        details_or_dict = details.details if isinstance(details, ipinfo.details.Details) else details

        country = details_or_dict.get("country", None)
        region = details_or_dict.get("region", None)

        region_string = ""  # Not technically necessary, but quiets the linter
        match (country is None, region is None):
            case (True, True):
                region_string = "unknown"
            case (True, False):
                region_string = region
            case (False, True):
                region_string = country
            case (False, False):
                region_string = f"{country}/{region}"

        ip_index = ip_address_to_ip_index_not_in_services[ip_address]
        index_to_region[ip_index] = region_string
    # return index_to_region_code
    # except ipinfo.exceptions.RequestQuotaExceededError:  # pragma: no cover
    #     return "TBD"

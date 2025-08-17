import ipaddress
import itertools
import math
import os

import ipinfo
import tqdm
import yaml

from ._globals import _KNOWN_SERVICES
from ._ip_cache import load_index_to_ip, load_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions
from ..config import get_ip_cache_directory


def update_index_to_region_codes(batch_size: int = 1_000) -> str | None:
    """Update the `indexed_region_codes.yaml` file in the cache directory."""
    ipinfo_api_key = os.environ.get("IPINFO_API_KEY", None)
    if ipinfo_api_key is None:
        message = "The environment variable 'IPINFO_API_KEY' must be set to import `s3_log_extraction`!"
        raise ValueError(message)  # pragma: no cover
    ipinfo_handler = ipinfo.getHandler(access_token=ipinfo_api_key)

    ip_cache_directory = get_ip_cache_directory()
    indexed_regions_file_path = ip_cache_directory / "index_to_region.yaml"

    index_to_ip = load_index_to_ip()
    index_not_in_services = load_ip_cache(cache_type="index_not_in_services")
    index_to_region = load_ip_cache(cache_type="index_to_region")
    indexes_to_update = set(index_to_ip.keys()) - set(index_to_region.keys())

    number_of_batches = math.ceil(len(indexes_to_update) / batch_size)
    for ip_index_batch in tqdm.tqdm(
        iterable=itertools.batched(iterable=indexes_to_update, n=batch_size),
        total=number_of_batches,
        desc="Fetching IP regions in batches",
        unit="batches",
        smoothing=0,
        position=0,
        leave=False,
    ):
        for ip_index in tqdm.tqdm(
            iterable=ip_index_batch,
            total=batch_size,
            desc="Fetching IP regions",
            unit=" IP addresses",
            smoothing=0,
            position=1,
            leave=False,
        ):
            ip_address = index_to_ip[ip_index]

            region_code = _get_region_code_from_ip_index(
                ip_index=ip_index,
                ip_address=ip_address,
                ipinfo_handler=ipinfo_handler,
                index_not_in_services=index_not_in_services,
            )

            if region_code is None:
                continue

            # API limit reached; do not cache and wait for it to reset
            if region_code == "TBD":
                continue
            index_to_region[ip_index] = region_code

            with indexed_regions_file_path.open(mode="w") as file_stream:
                yaml.dump(data=index_to_region, stream=file_stream)


def _get_region_code_from_ip_index(
    ip_index: int, ip_address: str, ipinfo_handler: ipinfo.Handler, index_not_in_services: dict[int, bool]
) -> str | None:
    # Determine if IP address belongs to GitHub, AWS, Google, or known VPNs
    # Azure not yet easily doable; keep an eye on
    # https://learn.microsoft.com/en-us/answers/questions/1410071/up-to-date-azure-public-api-to-get-azure-ip-ranges
    # maybe it will change in the future
    if index_not_in_services.get(ip_index, None) is None:
        for service_name in _KNOWN_SERVICES:
            cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)

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
                return region_service_string
    index_not_in_services[ip_index] = True

    # Lines cannot be covered without testing on a real IP
    try:  # pragma: no cover
        timeout_in_seconds = 10
        details = ipinfo_handler.getDetails(ip_address=ip_address, timeout=timeout_in_seconds)

        country = details.details.get("country", None)
        region = details.details.get("region", None)

        match (country is None, region is None):
            case (True, True):
                region_string = "bogon" if details.details.get("bogon", False) is True else None
            case (True, False):
                region_string = region
            case (False, True):
                region_string = country
            case (False, False):
                region_string = f"{country}/{region}"

        return region_string
    except ipinfo.exceptions.RequestQuotaExceededError:  # pragma: no cover
        return "TBD"

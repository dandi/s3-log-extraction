import itertools
import math
import os
import pathlib
import random
import typing
import warnings

import tqdm

from ._globals import _KNOWN_SERVICES
from ._ip_cache import load_ip_cache, write_ip_cache
from ._ip_utils import _get_cidr_address_ranges_and_subregions, _ip_in_cidr, _read_ips_from_file
from ..config import get_cache_directory


def update_ip_to_region_codes(
    batch_size: int = 1_000,
    batch_limit: int | None = None,
    cache_directory: str | pathlib.Path | None = None,
    use_encryption: bool = True,
) -> None:
    """
    Update the ``ip_to_region.yaml`` file in the cache directory.

    Parameters
    ----------
    batch_size : int
        Number of IP addresses to process in each batch.
        Default is 1,000.
    batch_limit : int | None
        Maximum number of batches to process.
        If `None`, all batches will be processed.
        Default is `None`.
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.
    use_encryption : bool
        If ``True`` (default), IP data files are decrypted when reading and encrypted when writing.
        If ``False``, IP data files are read and written as plaintext.
    """
    import ipinfo

    ipinfo_api_key = os.environ.get("IPINFO_API_KEY", None)
    if ipinfo_api_key is None:
        message = "The environment variable 'IPINFO_API_KEY' must be set to import `s3_log_extraction`!"
        raise ValueError(message)  # pragma: no cover
    ipinfo_handler = ipinfo.getHandler(access_token=ipinfo_api_key)

    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_dir / "extraction"
    extraction_directory.mkdir(exist_ok=True)
    all_ips: set[str] = set()
    for full_ips_file in tqdm.tqdm(
        iterable=extraction_directory.rglob(pattern="full_ips.txt"),
        desc="Reading IP files",
        unit=" files",
        smoothing=0,
    ):
        all_ips.update(_read_ips_from_file(file_path=full_ips_file, use_encryption=use_encryption))

    ip_to_region = load_ip_cache(
        cache_type="ip_to_region", cache_directory=cache_directory, use_encryption=use_encryption
    )
    ip_to_determined_region = {ip: region for ip, region in ip_to_region.items() if region != "undetermined"}
    ips_to_update = list(all_ips - set(ip_to_determined_region.keys()))

    # If a batch limit is set, shuffle the IPs to ensure repeated runs update different IPs
    if batch_limit is not None:
        random.shuffle(ips_to_update)

    number_of_batches = math.ceil(len(ips_to_update) / batch_size)
    if batch_limit is not None:
        number_of_batches = min(number_of_batches, batch_limit)
        ips_to_update = ips_to_update[: batch_limit * batch_size]

    for ip_batch in tqdm.tqdm(
        iterable=itertools.batched(iterable=ips_to_update, n=batch_size),
        total=number_of_batches,
        desc="Fetching IP regions in batches",
        unit="batches",
        smoothing=0,
        position=0,
        leave=False,
    ):
        for ip_address in tqdm.tqdm(
            iterable=ip_batch,
            total=batch_size,
            desc="Fetching IP regions",
            unit=" IP addresses",
            smoothing=0,
            position=1,
            leave=False,
        ):
            region_code = _get_region_code_from_ip_address(ip_address=ip_address, ipinfo_handler=ipinfo_handler)
            ip_to_region[ip_address] = region_code

            write_ip_cache(
                data=ip_to_region,
                cache_type="ip_to_region",
                cache_directory=cache_directory,
                use_encryption=use_encryption,
            )



def _get_region_code_from_ip_address(
    ip_address: str,
    ipinfo_handler: "ipinfo.Handler",
) -> str | typing.Literal["undetermined", "bogon"]:
    import ipinfo

    # Determine if the IP address belongs to GitHub, AWS, Google, or known VPNs
    # Azure not yet easily doable; keep an eye on
    # https://learn.microsoft.com/en-us/answers/questions/1410071/up-to-date-azure-public-api-to-get-azure-ip-ranges
    # maybe it will change in the future
    for service_name in _KNOWN_SERVICES:
        cidr_addresses_and_subregions = _get_cidr_address_ranges_and_subregions(service_name=service_name)

        matched_cidr_address_and_subregion = next(
            (
                (cidr_address, subregion)
                for cidr_address, subregion in cidr_addresses_and_subregions
                if _ip_in_cidr(ip_address=ip_address, cidr_address=cidr_address)
            ),
            None,
        )
        if matched_cidr_address_and_subregion is not None:
            region_service_string = service_name

            subregion = matched_cidr_address_and_subregion[1]
            if subregion is not None:
                region_service_string += f"/{subregion}"
            return region_service_string

    # TODO: add batching support to ipinfo requests
    # Lines cannot be covered without testing on a real IP
    try:  # pragma: no cover
        timeout_in_seconds = 30
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
        warnings.warn(
            msg="IPInfo API request quota exceeded. Returning 'undetermined' value.",
            category=RuntimeWarning,
            stacklevel=2,
        )
        return "undetermined"

import functools
import ipaddress
import pathlib
import re
import warnings

from ..utils.encryption import read_text_from_file, write_text_to_file

# Microsoft publishes the Azure ranges as a weekly file whose name carries its date, so there is no stable URL to it.
# The download page of the listing links to the current file.
_AZURE_DOWNLOAD_PAGE_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"
_AZURE_SERVICE_TAGS_URL_PATTERN = re.compile(
    r"https://download\.microsoft\.com/download/[^\"'\s]*?/ServiceTags_Public_\d+\.json"
)

_REQUEST_TIMEOUT_IN_SECONDS = 60


class ServiceNetworksWarning(UserWarning):
    """
    The published IP ranges of a known cloud service or VPN listing could not be obtained.

    The ranges only refine the region labels, so a source that is unreachable, answers with an error, or has changed
    its layout does not stop a run. Requesters in its ranges are labeled by the next service that lists them, or
    geolocated, instead. Turn this category into an error to make such a source fail loudly, for instance with
    ``-W error::s3_log_extraction.ip_utils.ServiceNetworksWarning`` as the test suite does.
    """


def _read_ips_from_file(file_path: pathlib.Path, use_encryption: bool = True) -> list[str]:
    """
    Read and return stripped, non-empty IP address strings from a ``ips.txt`` file.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the ``ips.txt`` file.
    use_encryption : bool, optional
        If ``True`` (default), the file content is decrypted before parsing.
        If ``False``, the file content is read as plaintext.
    """
    text = read_text_from_file(file_path=file_path, use_encryption=use_encryption)
    return [stripped for line in text.splitlines() if (stripped := line.strip())]


def _write_ips_to_file(file_path: pathlib.Path, ips: list[str], use_encryption: bool = True) -> None:
    """
    Write IP address strings to a ``ips.txt`` file, optionally encrypting the content.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the ``ips.txt`` file to write.
    ips : list of str
        IP address strings to write (one per line).
    use_encryption : bool, optional
        If ``True`` (default), the content is encrypted before writing.
        If ``False``, the content is written as plaintext.
    """
    text = "\n".join(ips) + ("\n" if ips else "")
    write_text_to_file(file_path=file_path, text=text, use_encryption=use_encryption)


def _request_published_listing(url: str, /) -> "requests.Response":
    """Request a published listing, raising on an error status so that an error page is never read as a listing."""
    import requests

    response = requests.get(url=url, timeout=_REQUEST_TIMEOUT_IN_SECONDS)
    response.raise_for_status()
    return response


@functools.lru_cache
def _request_cidr_range(service_name: str) -> dict | list[str]:
    """Cache (in-memory) the requests to external services."""
    match service_name:
        case "GitHub":
            github_cidr_request = _request_published_listing("https://api.github.com/meta").json()

            return github_cidr_request
        case "AWS":
            aws_cidr_request = _request_published_listing("https://ip-ranges.amazonaws.com/ip-ranges.json").json()

            return aws_cidr_request
        case "GCP":
            gcp_cidr_request = _request_published_listing("https://www.gstatic.com/ipranges/cloud.json").json()

            return gcp_cidr_request
        case "Azure":
            download_page = _request_published_listing(_AZURE_DOWNLOAD_PAGE_URL).text
            service_tags_url_match = _AZURE_SERVICE_TAGS_URL_PATTERN.search(download_page)
            if service_tags_url_match is None:
                message = (
                    f"Could not find the link to the Azure service tags file on {_AZURE_DOWNLOAD_PAGE_URL}. "
                    "The layout of the download page may have changed."
                )
                raise RuntimeError(message)
            azure_cidr_request = _request_published_listing(service_tags_url_match.group(0)).json()

            return azure_cidr_request
        case "VPN":
            # Very nice public and maintained listing! Hope this stays stable.
            vpn_cidr_request = (
                _request_published_listing(
                    "https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt"
                )
                .content.decode("utf-8")
                .splitlines()
            )

            return vpn_cidr_request
        case _:
            raise ValueError(f"Service name '{service_name}' is not supported!")  # pragma: no cover


def _is_ipv4_network(candidate: object, /) -> bool:
    """Whether a listed value is an IPv4 range rather than an IPv6 range, a key, a domain, or other metadata."""
    if not isinstance(candidate, str):
        return False
    try:
        return ipaddress.ip_network(address=candidate, strict=False).version == 4
    except ValueError:
        return False


@functools.lru_cache
def _get_cidr_address_ranges_and_subregions(*, service_name: str) -> list[tuple[str, str | None]]:
    """
    The published IPv4 ranges of a service as ``(cidr_address, subregion)`` pairs, or none when they are unavailable.

    A listing that cannot be requested or parsed, or that holds no IPv4 ranges, is reported with a
    ``ServiceNetworksWarning`` and taken as empty. The outcome is cached either way, so that a run reports a broken
    listing once rather than at every use.
    """
    try:
        cidr_request = _request_cidr_range(service_name=service_name)
        cidr_addresses_and_subregions = _parse_cidr_request(service_name=service_name, cidr_request=cidr_request)
    # The sources are outside the control of this package, and how one breaks cannot be known ahead of time
    except Exception as exception:  # noqa: BLE001
        reason = f"{type(exception).__name__}: {exception}"
        cidr_addresses_and_subregions = []
    else:
        reason = "The listing was obtained but holds no IPv4 ranges."

    if len(cidr_addresses_and_subregions) == 0:
        message = (
            f"The published IP ranges of {service_name} are unavailable for the rest of this run, so requesters in "
            f"them are labeled by the next service that lists them, or geolocated. {reason}"
        )
        warnings.warn(message=message, category=ServiceNetworksWarning, stacklevel=2)
    return cidr_addresses_and_subregions


def _parse_cidr_request(*, service_name: str, cidr_request: dict | list[str]) -> list[tuple[str, str | None]]:
    match service_name:
        case "GitHub":
            # The meta document lists the ranges of each GitHub product next to other metadata (domains, SSH keys,
            # PGP keys, ...) under keys that GitHub adds to over time, so the ranges are recognized by their shape
            # rather than by key: any string in a list that parses as an IPv4 network. IPv6 ranges are not handled.
            github_cidr_addresses_and_subregions = [
                (cidr_address, None)
                for value in cidr_request.values()
                if isinstance(value, list)
                for cidr_address in value
                if _is_ipv4_network(cidr_address)
            ]

            return github_cidr_addresses_and_subregions
        # Note: these endpoints also return the 'locations' of the specific subnet, such as 'us-east-2'
        case "AWS":
            aws_cidr_addresses_and_subregions = [
                (prefix["ip_prefix"], prefix.get("region", None)) for prefix in cidr_request["prefixes"]
            ]

            return aws_cidr_addresses_and_subregions
        case "GCP":
            gcp_cidr_addresses_and_subregions = [
                (prefix["ipv4Prefix"], prefix.get("scope", None))
                for prefix in cidr_request["prefixes"]
                if "ipv4Prefix" in prefix  # Not handling IPv6 yet
            ]

            return gcp_cidr_addresses_and_subregions
        case "Azure":
            # The regional "AzureCloud.<region>" tags carry the region. The "AzureCloud" tag spans every region, often
            # in aggregated blocks that no regional tag lists, so its ranges are kept without a region to label the
            # rest. A range listed by both is kept once, with its region.
            regional_cidr_addresses_and_subregions = [
                (cidr_address, service_tag["properties"]["region"] or None)
                for service_tag in cidr_request["values"]
                if service_tag["name"].startswith("AzureCloud.")
                for cidr_address in service_tag["properties"]["addressPrefixes"]
                if _is_ipv4_network(cidr_address)
            ]
            regional_cidr_addresses = {cidr_address for cidr_address, _ in regional_cidr_addresses_and_subregions}
            unregioned_cidr_addresses_and_subregions = [
                (cidr_address, None)
                for service_tag in cidr_request["values"]
                if service_tag["name"] == "AzureCloud"
                for cidr_address in service_tag["properties"]["addressPrefixes"]
                if _is_ipv4_network(cidr_address) and cidr_address not in regional_cidr_addresses
            ]
            azure_cidr_addresses_and_subregions = (
                unregioned_cidr_addresses_and_subregions + regional_cidr_addresses_and_subregions
            )

            return azure_cidr_addresses_and_subregions
        case "VPN":
            vpn_cidr_addresses_and_subregions = [(cidr_address, None) for cidr_address in cidr_request]

            return vpn_cidr_addresses_and_subregions
        case _:
            raise ValueError(f"Service name '{service_name}' is not supported!")  # pragma: no cover

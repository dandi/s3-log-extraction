import functools
import ipaddress
import pathlib
import re

from ..utils.encryption import read_text_from_file, write_text_to_file

# Microsoft publishes the Azure ranges as a weekly file whose name carries its date, so there is no stable URL to it.
# The download page of the listing links to the current file.
_AZURE_DOWNLOAD_PAGE_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"
_AZURE_SERVICE_TAGS_URL_PATTERN = re.compile(
    r"https://download\.microsoft\.com/download/[^\"'\s]*?/ServiceTags_Public_\d+\.json"
)


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


@functools.lru_cache
def _request_cidr_range(service_name: str) -> dict | list[str]:
    """Cache (in-memory) the requests to external services."""
    import requests

    match service_name:
        case "GitHub":
            github_cidr_request = requests.get(url="https://api.github.com/meta").json()

            return github_cidr_request
        case "AWS":
            aws_cidr_request = requests.get(url="https://ip-ranges.amazonaws.com/ip-ranges.json").json()

            return aws_cidr_request
        case "GCP":
            gcp_cidr_request = requests.get(url="https://www.gstatic.com/ipranges/cloud.json").json()

            return gcp_cidr_request
        case "Azure":
            download_page = requests.get(url=_AZURE_DOWNLOAD_PAGE_URL).text
            service_tags_url_match = _AZURE_SERVICE_TAGS_URL_PATTERN.search(download_page)
            if service_tags_url_match is None:
                message = (
                    f"Could not find the link to the Azure service tags file on {_AZURE_DOWNLOAD_PAGE_URL}. "
                    "The layout of the download page may have changed."
                )
                raise RuntimeError(message)
            azure_cidr_request = requests.get(url=service_tags_url_match.group(0)).json()

            return azure_cidr_request
        case "VPN":
            # Very nice public and maintained listing! Hope this stays stable.
            vpn_cidr_request = (
                requests.get(
                    url="https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt"
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
    cidr_request = _request_cidr_range(service_name=service_name)
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

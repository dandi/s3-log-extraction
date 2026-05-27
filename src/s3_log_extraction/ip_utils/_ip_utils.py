import functools
import ipaddress
import pathlib
import warnings


def _read_ips_from_file(file_path: pathlib.Path, use_encryption: bool = True) -> list[str]:
    """Read and return stripped, non-empty IP address strings from a ``full_ips.txt`` file.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the ``full_ips.txt`` file.
    use_encryption : bool, optional
        If ``True`` (default), the file content is decrypted before parsing.
        If ``False``, the file content is read as plaintext.
    """
    if use_encryption:
        from ..utils.encryption import decrypt_bytes

        raw_bytes = file_path.read_bytes()
        text = decrypt_bytes(raw_bytes).decode(encoding="utf-8")
    else:
        text = file_path.read_text()
    return [stripped for line in text.splitlines() if (stripped := line.strip())]


def _write_ips_to_file(file_path: pathlib.Path, ips: list[str], use_encryption: bool = True) -> None:
    """Write IP address strings to a ``full_ips.txt`` file, optionally encrypting the content.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the ``full_ips.txt`` file to write.
    ips : list of str
        IP address strings to write (one per line).
    use_encryption : bool, optional
        If ``True`` (default), the content is encrypted before writing.
        If ``False``, the content is written as plaintext.
    """
    text = "\n".join(ips) + ("\n" if ips else "")
    if use_encryption:
        from ..utils.encryption import encrypt_bytes

        file_path.write_bytes(encrypt_bytes(text.encode(encoding="utf-8")))
    else:
        file_path.write_text(text)


def _ip_in_cidr(ip_address: str, cidr_address: str) -> bool:
    """Return True if ``ip_address`` falls within ``cidr_address``, False otherwise.

    Uses ``strict=False`` to accept CIDRs that have host bits set, and returns
    ``False`` for any entry that is not a valid CIDR string.
    """
    try:
        return ipaddress.ip_address(address=ip_address) in ipaddress.ip_network(address=cidr_address, strict=False)
    except ValueError as exception:
        warnings.warn(
            message=(f"Skipping invalid CIDR entry {cidr_address!r} while checking IP {ip_address!r}: {exception}"),
            stacklevel=2,
        )
        return False


@functools.lru_cache
def _request_cidr_range(service_name: str) -> dict:
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
            raise NotImplementedError("Azure CIDR address fetching is not yet implemented!")
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


@functools.lru_cache
def _get_cidr_address_ranges_and_subregions(*, service_name: str) -> list[tuple[str, str | None]]:
    cidr_request = _request_cidr_range(service_name=service_name)
    match service_name:
        case "GitHub":
            skip_keys = ["domains", "ssh_key_fingerprints", "verifiable_password_authentication", "ssh_keys"]
            keys = set(cidr_request.keys()) - set(skip_keys)
            github_cidr_addresses_and_subregions = [
                (cidr_address, None)
                for key in keys
                for cidr_address in cidr_request[key]
                if "::" not in cidr_address
                # Skip IPv6
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
            raise NotImplementedError("Azure CIDR address fetching is not yet implemented!")  # pragma: no cover
        case "VPN":
            vpn_cidr_addresses_and_subregions = [(cidr_address, None) for cidr_address in cidr_request]

            return vpn_cidr_addresses_and_subregions
        case _:
            raise ValueError(f"Service name '{service_name}' is not supported!")  # pragma: no cover

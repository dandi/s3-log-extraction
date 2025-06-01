import functools

import requests


@functools.lru_cache
def _request_cidr_range(service_name: str) -> dict:
    """Cache (in-memory) the requests to external services."""
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

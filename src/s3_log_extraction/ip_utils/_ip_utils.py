import datetime
import functools
import ipaddress
import json
import pathlib
import re
import warnings

from ..config import get_cache_subdirectory
from ..utils.encryption import read_text_from_file, write_text_to_file

# Microsoft publishes the Azure ranges as a weekly file whose name carries its date, so there is no stable URL to it.
# Every week's file lands in the same folder of the download server, so the newest one is found by trying the dates
# around today there, newest first. The download page that links to the current file sits behind bot protection that
# at times turns away cloud and CI machines, so it is only tried when no dated file turns up, as after a move of the
# folder.
_AZURE_SERVICE_TAGS_FOLDER_URL = "https://download.microsoft.com/download/7/1/d/71d86715-5596-4529-9b13-da13a5de5b63"
_AZURE_SERVICE_TAGS_LOOKAHEAD_IN_DAYS = 7
_AZURE_SERVICE_TAGS_LOOKBACK_IN_DAYS = 28
_AZURE_DOWNLOAD_PAGE_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"
_AZURE_SERVICE_TAGS_URL_PATTERN = re.compile(
    r"https://download\.microsoft\.com/download/[^\"'\s]*?/ServiceTags_Public_\d+\.json"
)
_NOT_FOUND_STATUS_CODE = 404

# The last listing obtained from each source is kept in the cache directory and used when the source cannot be reached.
# The published ranges change slowly, so a copy this recent labels requesters nearly as well as a fresh one.
_SAVED_LISTING_MAX_AGE_IN_DAYS = 30
_SAVED_LISTINGS_SUBDIRECTORY_NAME = "service_networks"

_REQUEST_TIMEOUT_IN_SECONDS = 60


class ServiceNetworksWarning(UserWarning):
    """
    The published IP ranges of a known cloud service or VPN listing could not be obtained.

    The ranges only refine the region labels, so a source that is unreachable, answers with an error, or has changed
    its layout does not stop a run. The last listing saved in the cache directory is used in its place when it is
    recent enough. Otherwise requesters in its ranges are labeled by the next service that lists them, or geolocated,
    instead. Turn this category into an error to make such a source fail loudly, for instance with
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
            azure_cidr_request = _request_azure_service_tags()

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


def _utc_today() -> datetime.date:
    """Today's date in UTC, which the Azure file names follow. A function of its own so that tests can fix the date."""
    today = datetime.datetime.now(tz=datetime.UTC).date()
    return today


def _request_azure_service_tags() -> dict:
    """
    Request the newest weekly Azure service tags file, directly from the download server where possible.

    The dated file names are tried in the download folder from a week ahead of today back to four weeks before it,
    newest first, and the first one answered with JSON is used. Only when none is found, or the download server fails,
    is the download page read for the link to the current file.
    """
    import requests

    today = _utc_today()
    direct_failure = (
        f"No file dated within {_AZURE_SERVICE_TAGS_LOOKBACK_IN_DAYS} days of today was found in "
        f"{_AZURE_SERVICE_TAGS_FOLDER_URL}."
    )
    for days_before_today in range(-_AZURE_SERVICE_TAGS_LOOKAHEAD_IN_DAYS, _AZURE_SERVICE_TAGS_LOOKBACK_IN_DAYS + 1):
        file_date = today - datetime.timedelta(days=days_before_today)
        service_tags_url = f"{_AZURE_SERVICE_TAGS_FOLDER_URL}/ServiceTags_Public_{file_date:%Y%m%d}.json"
        try:
            azure_service_tags = _request_published_listing(service_tags_url).json()
        except requests.HTTPError as exception:
            if getattr(exception.response, "status_code", None) == _NOT_FOUND_STATUS_CODE:
                continue
            direct_failure = f"{type(exception).__name__}: {exception}"
            break
        # A date with no file may also be answered with a page rather than a 404, which is not JSON either.
        # Listed ahead of `RequestException`, since the JSON error of `requests` is one of those as well.
        except ValueError:
            continue
        except requests.RequestException as exception:
            direct_failure = f"{type(exception).__name__}: {exception}"
            break
        return azure_service_tags

    try:
        download_page = _request_published_listing(_AZURE_DOWNLOAD_PAGE_URL).text
    except requests.RequestException as exception:
        message = (
            f"The Azure service tags file was not found on the download server ({direct_failure}), and its download "
            f"page could not be read either ({type(exception).__name__}: {exception})."
        )
        raise RuntimeError(message) from exception
    service_tags_url_match = _AZURE_SERVICE_TAGS_URL_PATTERN.search(download_page)
    if service_tags_url_match is None:
        message = (
            f"The Azure service tags file was not found on the download server ({direct_failure}). "
            f"Could not find the link to the Azure service tags file on {_AZURE_DOWNLOAD_PAGE_URL} either. "
            "The layout of the download page may have changed."
        )
        raise RuntimeError(message)
    azure_service_tags = _request_published_listing(service_tags_url_match.group(0)).json()
    return azure_service_tags


def _get_saved_listing_file_path(
    *, service_name: str, cache_directory: str | pathlib.Path | None = None
) -> pathlib.Path:
    """Where the last listing obtained for a service is kept."""
    saved_listings_directory = get_cache_subdirectory(
        cache_directory=cache_directory, name=_SAVED_LISTINGS_SUBDIRECTORY_NAME
    )
    saved_listing_file_path = saved_listings_directory / f"{service_name}.json"
    return saved_listing_file_path


def _save_listing(
    *, saved_listing_file_path: pathlib.Path, cidr_addresses_and_subregions: list[tuple[str, str | None]]
) -> None:
    """Keep a listing, replacing the saved copy atomically so that a reader never sees a partial file."""
    temporary_file_path = saved_listing_file_path.with_name(f"{saved_listing_file_path.name}.tmp")
    temporary_file_path.write_text(json.dumps(cidr_addresses_and_subregions))
    temporary_file_path.replace(saved_listing_file_path)


def _load_saved_listing(saved_listing_file_path: pathlib.Path, /) -> tuple[list[tuple[str, str | None]], float] | None:
    """
    The saved copy of a listing and its age in days, or ``None`` when there is no recent enough copy to use.

    A copy older than ``_SAVED_LISTING_MAX_AGE_IN_DAYS``, or one that cannot be read, is not used.
    """
    if not saved_listing_file_path.exists():
        return None

    modified = datetime.datetime.fromtimestamp(saved_listing_file_path.stat().st_mtime, tz=datetime.UTC)
    age_in_days = (datetime.datetime.now(tz=datetime.UTC) - modified) / datetime.timedelta(days=1)
    if age_in_days > _SAVED_LISTING_MAX_AGE_IN_DAYS:
        return None

    try:
        cidr_addresses_and_subregions = [
            (cidr_address, subregion) for cidr_address, subregion in json.loads(saved_listing_file_path.read_text())
        ]
    except ValueError, TypeError:
        return None
    if len(cidr_addresses_and_subregions) == 0:
        return None

    saved_listing = (cidr_addresses_and_subregions, age_in_days)
    return saved_listing


def _is_ipv4_network(candidate: object, /) -> bool:
    """Whether a listed value is an IPv4 range rather than an IPv6 range, a key, a domain, or other metadata."""
    if not isinstance(candidate, str):
        return False
    try:
        return ipaddress.ip_network(address=candidate, strict=False).version == 4
    except ValueError:
        return False


@functools.lru_cache
def _get_cidr_address_ranges_and_subregions(
    *, service_name: str, cache_directory: str | pathlib.Path | None = None
) -> list[tuple[str, str | None]]:
    """
    The published IPv4 ranges of a service as ``(cidr_address, subregion)`` pairs, or none when they are unavailable.

    Every listing obtained is saved in the cache directory. A listing that cannot be requested or parsed, or that
    holds no IPv4 ranges, is reported with a ``ServiceNetworksWarning``, and the saved copy is used in its place when
    one is at most ``_SAVED_LISTING_MAX_AGE_IN_DAYS`` old. Without such a copy the ranges are taken as empty. The
    outcome is cached either way, so that a run reports a broken listing once rather than at every use.
    """
    saved_listing_file_path = _get_saved_listing_file_path(service_name=service_name, cache_directory=cache_directory)
    try:
        cidr_request = _request_cidr_range(service_name=service_name)
        cidr_addresses_and_subregions = _parse_cidr_request(service_name=service_name, cidr_request=cidr_request)
    # The sources are outside the control of this package, and how one breaks cannot be known ahead of time
    except Exception as exception:  # noqa: BLE001
        reason = f"{type(exception).__name__}: {exception}"
        cidr_addresses_and_subregions = []
    else:
        reason = "The listing was obtained but holds no IPv4 ranges."

    if len(cidr_addresses_and_subregions) > 0:
        _save_listing(
            saved_listing_file_path=saved_listing_file_path, cidr_addresses_and_subregions=cidr_addresses_and_subregions
        )
        return cidr_addresses_and_subregions

    saved_listing = _load_saved_listing(saved_listing_file_path)
    if saved_listing is not None:
        saved_cidr_addresses_and_subregions, age_in_days = saved_listing
        message = (
            f"The published IP ranges of {service_name} could not be obtained, so the copy saved "
            f"{age_in_days:.1f} days ago at {saved_listing_file_path} is used for the rest of this run. {reason}"
        )
        warnings.warn(message=message, category=ServiceNetworksWarning, stacklevel=2)
        return saved_cidr_addresses_and_subregions

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

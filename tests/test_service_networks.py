"""Tests for fetching the published IP ranges of the known cloud services and VPN listings."""

import warnings

import pytest
import requests

import s3_log_extraction
from s3_log_extraction.ip_utils import ServiceNetworksWarning, fetch_service_networks

_GITHUB_META = {"hooks": ["192.0.2.0/24"], "domains": {"website": ["*.github.com"]}}
_AWS_RANGES = {
    "prefixes": [
        {"ip_prefix": "198.51.100.0/24", "region": "us-east-1"},
        {"ip_prefix": "198.51.100.0/25"},  # No region reported
    ]
}
_GCP_RANGES = {
    "prefixes": [
        {"ipv4Prefix": "203.0.113.0/24", "scope": "us-central1"},
        {"ipv6Prefix": "2001:db8::/32"},  # IPv6 is not handled
    ]
}
_AZURE_DOWNLOAD_PAGE_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"
_AZURE_SERVICE_TAGS_URL = "https://download.microsoft.com/download/0/1/2/012abc/ServiceTags_Public_20260928.json"
_AZURE_DOWNLOAD_PAGE = f'<html><a href="{_AZURE_SERVICE_TAGS_URL}" class="download">Download</a></html>'
_AZURE_SERVICE_TAGS = {
    "values": [
        {
            "name": "AzureCloud",
            "properties": {"region": "", "addressPrefixes": ["198.51.100.0/24", "192.0.2.0/26", "2001:db8::/32"]},
        },
        {"name": "AzureCloud.eastus", "properties": {"region": "eastus", "addressPrefixes": ["192.0.2.0/26"]}},
        {
            "name": "AzureCloud.westeurope",
            "properties": {"region": "westeurope", "addressPrefixes": ["203.0.113.0/25"]},
        },
        {"name": "Storage", "properties": {"region": "", "addressPrefixes": ["192.0.2.64/26"]}},  # Not a region tag
    ]
}
_VPN_RANGES = "192.0.2.128/25\n198.51.100.128/25\n"

# The first URL requested for each service, which is the one a test breaks to make that listing unavailable
_SERVICE_NAME_TO_URL = {
    "GitHub": "https://api.github.com/meta",
    "AWS": "https://ip-ranges.amazonaws.com/ip-ranges.json",
    "GCP": "https://www.gstatic.com/ipranges/cloud.json",
    "Azure": _AZURE_DOWNLOAD_PAGE_URL,
    "VPN": "https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt",
}
_URL_TO_PAYLOAD = {
    _SERVICE_NAME_TO_URL["GitHub"]: _GITHUB_META,
    _SERVICE_NAME_TO_URL["AWS"]: _AWS_RANGES,
    _SERVICE_NAME_TO_URL["GCP"]: _GCP_RANGES,
    _AZURE_DOWNLOAD_PAGE_URL: _AZURE_DOWNLOAD_PAGE,
    _AZURE_SERVICE_TAGS_URL: _AZURE_SERVICE_TAGS,
    _SERVICE_NAME_TO_URL["VPN"]: _VPN_RANGES,
}
_EXPECTED_SERVICE_NETWORKS = {
    "GitHub": [("192.0.2.0/24", None)],
    "AWS": [("198.51.100.0/24", "us-east-1"), ("198.51.100.0/25", None)],
    "GCP": [("203.0.113.0/24", "us-central1")],
    "Azure": [("198.51.100.0/24", None), ("192.0.2.0/26", "eastus"), ("203.0.113.0/25", "westeurope")],
    "VPN": [("192.0.2.128/25", None), ("198.51.100.128/25", None)],
}


class _FakeResponse:
    """
    A stand-in for `requests.Response` exposing only what the range fetching reads.

    A payload that is a `requests.HTTPError` stands for an error status, raised when the status is checked.
    """

    def __init__(self, *, payload: dict | str | requests.HTTPError) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        if isinstance(self._payload, requests.HTTPError):
            raise self._payload

    def json(self) -> dict:
        return self._payload

    @property
    def content(self) -> bytes:
        return self._payload.encode(encoding="utf-8")

    @property
    def text(self) -> str:
        return self._payload


@pytest.fixture
def url_to_payload() -> dict[str, dict | str | requests.RequestException]:
    """
    The payload served for each published URL, which a test may change before the listings are fetched.

    A `requests.ConnectionError` is raised by the request itself, and a `requests.HTTPError` when its status is checked.
    """
    return dict(_URL_TO_PAYLOAD)


@pytest.fixture
def mocked_service_listings(
    monkeypatch: pytest.MonkeyPatch, url_to_payload: dict[str, dict | str | requests.RequestException]
) -> list[str]:
    """Serve the published listings from fixtures rather than over the network, and report the URLs requested."""
    requested_urls = []
    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()

    # The timeout is required, so that a request made without one fails here
    def _fake_get(url: str, timeout: float) -> _FakeResponse:
        requested_urls.append(url)
        payload = url_to_payload[url]
        if isinstance(payload, requests.ConnectionError):
            raise payload
        return _FakeResponse(payload=payload)

    monkeypatch.setattr(requests, "get", _fake_get)
    yield requested_urls

    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()


@pytest.mark.ai_generated
def test_fetch_service_networks_covers_every_known_service(mocked_service_listings: list[str]) -> None:
    """Every known service should be fetched from its own published endpoint."""
    service_networks = fetch_service_networks()

    assert list(service_networks.keys()) == ["GitHub", "AWS", "GCP", "Azure", "VPN"]  # In order of precedence
    assert sorted(mocked_service_listings) == sorted(_URL_TO_PAYLOAD.keys())


@pytest.mark.ai_generated
@pytest.mark.parametrize("service_name", list(_EXPECTED_SERVICE_NETWORKS.keys()))
def test_fetch_service_networks_parses_each_listing(mocked_service_listings: list[str], service_name: str) -> None:
    """Each listing has its own shape, and the subregion is carried along where the service reports one."""
    service_networks = fetch_service_networks()

    assert service_networks[service_name] == _EXPECTED_SERVICE_NETWORKS[service_name]


@pytest.mark.ai_generated
def test_fetch_service_networks_is_cached(mocked_service_listings: list[str]) -> None:
    """The published listings should be requested only once per session."""
    fetch_service_networks()
    fetch_service_networks()

    assert len(mocked_service_listings) == len(_URL_TO_PAYLOAD)


@pytest.mark.ai_generated
@pytest.mark.parametrize("service_name", list(_SERVICE_NAME_TO_URL.keys()))
def test_unavailable_listing_warns_and_keeps_the_other_services(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    service_name: str,
) -> None:
    """An unreachable source leaves only its own service without ranges, and warns instead of failing the run."""
    url_to_payload[_SERVICE_NAME_TO_URL[service_name]] = requests.ConnectionError("Connection refused")

    with pytest.warns(ServiceNetworksWarning, match=f"The published IP ranges of {service_name} are unavailable"):
        service_networks = fetch_service_networks()

    expected_service_networks = {**_EXPECTED_SERVICE_NETWORKS, service_name: []}
    assert service_networks == expected_service_networks


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("url", "payload", "expected_reason"),
    [
        pytest.param(
            _SERVICE_NAME_TO_URL["GitHub"],
            requests.HTTPError("403 Client Error: rate limit exceeded"),
            "HTTPError: 403 Client Error",
            id="error-status",
        ),
        pytest.param(
            _SERVICE_NAME_TO_URL["AWS"], {"syncToken": "1"}, "KeyError: 'prefixes'", id="changed-listing-layout"
        ),
        pytest.param(
            _AZURE_DOWNLOAD_PAGE_URL,
            "<html>No link here</html>",
            "Could not find the link to the Azure service tags file",
            id="changed-download-page",
        ),
        pytest.param(_SERVICE_NAME_TO_URL["VPN"], "", "holds no IPv4 ranges", id="empty-listing"),
    ],
)
def test_unavailable_listing_warning_names_the_cause(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    url: str,
    payload: dict | str | requests.RequestException,
    expected_reason: str,
) -> None:
    """However a listing breaks, the warning should say how, so that the source can be fixed."""
    url_to_payload[url] = payload

    with pytest.warns(ServiceNetworksWarning, match=expected_reason):
        fetch_service_networks()


@pytest.mark.ai_generated
def test_unavailable_listing_is_reported_once(
    url_to_payload: dict[str, dict | str | requests.RequestException], mocked_service_listings: list[str]
) -> None:
    """A broken listing should be tried and reported once per run, not again by every later fetch."""
    url_to_payload[_AZURE_DOWNLOAD_PAGE_URL] = requests.ConnectionError("Connection refused")

    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter(action="always", category=ServiceNetworksWarning)
        fetch_service_networks()
        fetch_service_networks()

    assert [caught_warning.category for caught_warning in caught_warnings] == [ServiceNetworksWarning]
    assert mocked_service_listings.count(_AZURE_DOWNLOAD_PAGE_URL) == 1


@pytest.mark.ai_generated
def test_unavailable_listing_fails_the_tests(
    url_to_payload: dict[str, dict | str | requests.RequestException], mocked_service_listings: list[str]
) -> None:
    """The test configuration turns the warning into an error, so that a test meeting a broken source fails."""
    url_to_payload[_AZURE_DOWNLOAD_PAGE_URL] = requests.ConnectionError("Connection refused")

    with pytest.raises(ServiceNetworksWarning, match="The published IP ranges of Azure are unavailable"):
        fetch_service_networks()


@pytest.mark.remote
@pytest.mark.ai_generated
def test_every_published_listing_is_available_remote() -> None:
    """
    Every published listing should be obtainable live, and hold IPv4 ranges.

    A broken source only warns at run time, so this is where the daily remote tests catch it, independently of the
    MaxMind credentials that the other remote tests need. Every warning is collected, rather than raised at the first,
    so that one run names every source that has broken.
    """
    with warnings.catch_warnings(record=True) as caught_warnings:
        warnings.simplefilter(action="always", category=ServiceNetworksWarning)
        service_networks = fetch_service_networks()

    unavailable_listing_messages = [
        str(caught_warning.message)
        for caught_warning in caught_warnings
        if issubclass(caught_warning.category, ServiceNetworksWarning)
    ]
    assert unavailable_listing_messages == []
    assert list(service_networks.keys()) == ["GitHub", "AWS", "GCP", "Azure", "VPN"]

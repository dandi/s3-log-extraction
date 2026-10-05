"""Tests for fetching the published IP ranges of the known cloud services and VPN listings."""

import pytest
import requests

import s3_log_extraction
from s3_log_extraction.ip_utils import fetch_service_networks

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

_URL_TO_PAYLOAD = {
    "https://api.github.com/meta": _GITHUB_META,
    "https://ip-ranges.amazonaws.com/ip-ranges.json": _AWS_RANGES,
    "https://www.gstatic.com/ipranges/cloud.json": _GCP_RANGES,
    _AZURE_DOWNLOAD_PAGE_URL: _AZURE_DOWNLOAD_PAGE,
    _AZURE_SERVICE_TAGS_URL: _AZURE_SERVICE_TAGS,
    "https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt": _VPN_RANGES,
}


class _FakeResponse:
    """A stand-in for `requests.Response` exposing only what the range fetching reads."""

    def __init__(self, *, payload: dict | str) -> None:
        self._payload = payload

    def json(self) -> dict:
        return self._payload

    @property
    def content(self) -> bytes:
        return self._payload.encode(encoding="utf-8")

    @property
    def text(self) -> str:
        return self._payload


@pytest.fixture
def url_to_payload() -> dict[str, dict | str]:
    """The payload served for each published URL, which a test may change before the listings are fetched."""
    return dict(_URL_TO_PAYLOAD)


@pytest.fixture
def mocked_service_listings(monkeypatch: pytest.MonkeyPatch, url_to_payload: dict[str, dict | str]) -> list[str]:
    """Serve the published listings from fixtures rather than over the network, and report the URLs requested."""
    requested_urls = []
    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()

    def _fake_get(url: str) -> _FakeResponse:
        requested_urls.append(url)
        return _FakeResponse(payload=url_to_payload[url])

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
@pytest.mark.parametrize(
    ("service_name", "expected_networks"),
    [
        ("GitHub", [("192.0.2.0/24", None)]),
        ("AWS", [("198.51.100.0/24", "us-east-1"), ("198.51.100.0/25", None)]),
        ("GCP", [("203.0.113.0/24", "us-central1")]),
        ("Azure", [("198.51.100.0/24", None), ("192.0.2.0/26", "eastus"), ("203.0.113.0/25", "westeurope")]),
        ("VPN", [("192.0.2.128/25", None), ("198.51.100.128/25", None)]),
    ],
)
def test_fetch_service_networks_parses_each_listing(
    mocked_service_listings: list[str], service_name: str, expected_networks: list[tuple[str, str | None]]
) -> None:
    """Each listing has its own shape, and the subregion is carried along where the service reports one."""
    service_networks = fetch_service_networks()

    assert service_networks[service_name] == expected_networks


@pytest.mark.ai_generated
def test_fetch_service_networks_is_cached(mocked_service_listings: list[str]) -> None:
    """The published listings should be requested only once per session."""
    fetch_service_networks()
    fetch_service_networks()

    assert len(mocked_service_listings) == len(_URL_TO_PAYLOAD)


@pytest.mark.ai_generated
def test_azure_listing_without_a_download_link_fails_clearly(
    url_to_payload: dict[str, dict | str], mocked_service_listings: list[str]
) -> None:
    """A change to the Azure download page that hides the weekly file should say so, not fail on parsing."""
    url_to_payload[_AZURE_DOWNLOAD_PAGE_URL] = "<html>No link here</html>"

    with pytest.raises(RuntimeError, match="Could not find the link to the Azure service tags file"):
        fetch_service_networks()

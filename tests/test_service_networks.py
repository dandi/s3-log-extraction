"""Tests for fetching the published IP ranges of the known cloud services and VPN listings."""

import datetime
import json
import os
import pathlib
import time
import types
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

# The Azure file is looked for by date, so the tests fix the date the lookup starts from
_TODAY = datetime.date(2026, 9, 30)
_AZURE_SERVICE_TAGS_FOLDER_URL = "https://download.microsoft.com/download/7/1/d/71d86715-5596-4529-9b13-da13a5de5b63"
_AZURE_SERVICE_TAGS_URL = f"{_AZURE_SERVICE_TAGS_FOLDER_URL}/ServiceTags_Public_20260928.json"
_AZURE_PREVIOUS_SERVICE_TAGS_URL = f"{_AZURE_SERVICE_TAGS_FOLDER_URL}/ServiceTags_Public_20260921.json"
_AZURE_DOWNLOAD_PAGE_URL = "https://www.microsoft.com/en-us/download/details.aspx?id=56519"
# What the download page links to, standing for a folder that has moved since the package was written
_AZURE_MOVED_SERVICE_TAGS_URL = "https://download.microsoft.com/download/0/1/2/012abc/ServiceTags_Public_20260928.json"
_AZURE_DOWNLOAD_PAGE = f'<html><a href="{_AZURE_MOVED_SERVICE_TAGS_URL}" class="download">Download</a></html>'
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

# The URLs a test breaks to make the listing of each service unavailable. Azure has a fallback to break as well.
_SERVICE_NAME_TO_URLS = {
    "GitHub": ("https://api.github.com/meta",),
    "AWS": ("https://ip-ranges.amazonaws.com/ip-ranges.json",),
    "GCP": ("https://www.gstatic.com/ipranges/cloud.json",),
    "Azure": (_AZURE_SERVICE_TAGS_URL, _AZURE_DOWNLOAD_PAGE_URL),
    "VPN": ("https://raw.githubusercontent.com/josephrocca/is-vpn/main/vpn-or-datacenter-ipv4-ranges.txt",),
}
# The listing each service is read from while every source works
_PRIMARY_URLS = [urls[0] for urls in _SERVICE_NAME_TO_URLS.values()]
_URL_TO_PAYLOAD = {
    _SERVICE_NAME_TO_URLS["GitHub"][0]: _GITHUB_META,
    _SERVICE_NAME_TO_URLS["AWS"][0]: _AWS_RANGES,
    _SERVICE_NAME_TO_URLS["GCP"][0]: _GCP_RANGES,
    _AZURE_SERVICE_TAGS_URL: _AZURE_SERVICE_TAGS,
    _AZURE_DOWNLOAD_PAGE_URL: _AZURE_DOWNLOAD_PAGE,
    _AZURE_MOVED_SERVICE_TAGS_URL: _AZURE_SERVICE_TAGS,
    _SERVICE_NAME_TO_URLS["VPN"][0]: _VPN_RANGES,
}
_EXPECTED_SERVICE_NETWORKS = {
    "GitHub": [("192.0.2.0/24", None)],
    "AWS": [("198.51.100.0/24", "us-east-1"), ("198.51.100.0/25", None)],
    "GCP": [("203.0.113.0/24", "us-central1")],
    "Azure": [("198.51.100.0/24", None), ("192.0.2.0/26", "eastus"), ("203.0.113.0/25", "westeurope")],
    "VPN": [("192.0.2.128/25", None), ("198.51.100.128/25", None)],
}


def _azure_service_tags_url(file_date: datetime.date) -> str:
    return f"{_AZURE_SERVICE_TAGS_FOLDER_URL}/ServiceTags_Public_{file_date:%Y%m%d}.json"


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
        if isinstance(self._payload, str):
            raise requests.JSONDecodeError("Expecting value", self._payload, 0)
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
    Any URL left out is answered with a 404, as the download server answers a date no file was published for.
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
        payload = url_to_payload.get(
            url, requests.HTTPError("404 Client Error: Not Found", response=types.SimpleNamespace(status_code=404))
        )
        if isinstance(payload, requests.ConnectionError):
            raise payload
        return _FakeResponse(payload=payload)

    monkeypatch.setattr(requests, "get", _fake_get)
    monkeypatch.setattr(s3_log_extraction.ip_utils._ip_utils, "_utc_today", lambda: _TODAY)
    yield requested_urls

    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()


def _break_listing(url_to_payload: dict[str, dict | str | requests.RequestException], service_name: str) -> None:
    for url in _SERVICE_NAME_TO_URLS[service_name]:
        url_to_payload[url] = requests.ConnectionError("Connection refused")


def _start_new_run() -> None:
    """Forget what this process fetched, as a new run of the tools would."""
    s3_log_extraction.ip_utils._ip_utils._request_cidr_range.cache_clear()
    s3_log_extraction.ip_utils._ip_utils._get_cidr_address_ranges_and_subregions.cache_clear()


@pytest.mark.ai_generated
def test_fetch_service_networks_covers_every_known_service(mocked_service_listings: list[str]) -> None:
    """Every known service should be fetched from its own published listing, and Azure without its download page."""
    service_networks = fetch_service_networks()

    assert list(service_networks.keys()) == ["GitHub", "AWS", "GCP", "Azure", "VPN"]  # In order of precedence
    published_urls_requested = [url for url in mocked_service_listings if url in _URL_TO_PAYLOAD]
    assert sorted(published_urls_requested) == sorted(_PRIMARY_URLS)


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
    request_count_after_first_fetch = len(mocked_service_listings)
    fetch_service_networks()

    assert len(mocked_service_listings) == request_count_after_first_fetch


@pytest.mark.ai_generated
def test_azure_file_is_found_by_date_newest_first(
    url_to_payload: dict[str, dict | str | requests.RequestException], mocked_service_listings: list[str]
) -> None:
    """The dated file names are tried from a week ahead of today backward, and the newest file found is used."""
    url_to_payload[_AZURE_PREVIOUS_SERVICE_TAGS_URL] = {"values": []}

    service_networks = fetch_service_networks()

    expected_azure_urls = [
        _azure_service_tags_url(_TODAY - datetime.timedelta(days=days_before_today))
        for days_before_today in range(-7, 3)
    ]
    azure_urls_requested = [url for url in mocked_service_listings if url.startswith(_AZURE_SERVICE_TAGS_FOLDER_URL)]
    assert azure_urls_requested == expected_azure_urls
    assert expected_azure_urls[-1] == _AZURE_SERVICE_TAGS_URL
    assert service_networks["Azure"] == _EXPECTED_SERVICE_NETWORKS["Azure"]


@pytest.mark.ai_generated
def test_azure_dates_answered_with_a_page_are_skipped(
    url_to_payload: dict[str, dict | str | requests.RequestException], mocked_service_listings: list[str]
) -> None:
    """A date answered with a page rather than a 404 counts as one with no file, and the lookup goes on."""
    for days_before_today in range(-7, 2):
        url_to_payload[_azure_service_tags_url(_TODAY - datetime.timedelta(days=days_before_today))] = (
            "<html>We could not find that download.</html>"
        )

    service_networks = fetch_service_networks()

    assert service_networks["Azure"] == _EXPECTED_SERVICE_NETWORKS["Azure"]
    assert _AZURE_SERVICE_TAGS_URL in mocked_service_listings
    assert _AZURE_DOWNLOAD_PAGE_URL not in mocked_service_listings


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "direct_payload",
    [
        pytest.param(None, id="no-dated-file-published"),
        pytest.param(requests.ConnectionError("Connection refused"), id="download-server-unreachable"),
        pytest.param(requests.HTTPError("403 Client Error: Forbidden"), id="download-server-refuses"),
    ],
)
def test_azure_falls_back_to_the_download_page(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    direct_payload: requests.RequestException | None,
) -> None:
    """When no dated file is found, or the download server fails, the file the download page links to is used."""
    first_azure_url = _azure_service_tags_url(_TODAY + datetime.timedelta(days=7))
    del url_to_payload[_AZURE_SERVICE_TAGS_URL]
    if direct_payload is not None:
        url_to_payload[first_azure_url] = direct_payload

    service_networks = fetch_service_networks()

    assert service_networks["Azure"] == _EXPECTED_SERVICE_NETWORKS["Azure"]
    assert mocked_service_listings.count(_AZURE_DOWNLOAD_PAGE_URL) == 1
    assert mocked_service_listings.count(_AZURE_MOVED_SERVICE_TAGS_URL) == 1
    azure_urls_requested = [url for url in mocked_service_listings if url.startswith(_AZURE_SERVICE_TAGS_FOLDER_URL)]
    if direct_payload is None:
        assert len(azure_urls_requested) == 7 + 28 + 1  # Every date in the window was tried
    else:
        assert azure_urls_requested == [first_azure_url]  # A failing server is not tried again for every date


@pytest.mark.ai_generated
@pytest.mark.parametrize("service_name", list(_SERVICE_NAME_TO_URLS.keys()))
def test_unavailable_listing_warns_and_keeps_the_other_services(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    service_name: str,
) -> None:
    """An unreachable source leaves only its own service without ranges, and warns instead of failing the run."""
    _break_listing(url_to_payload=url_to_payload, service_name=service_name)

    with pytest.warns(ServiceNetworksWarning, match=f"The published IP ranges of {service_name} are unavailable"):
        service_networks = fetch_service_networks()

    expected_service_networks = {**_EXPECTED_SERVICE_NETWORKS, service_name: []}
    assert service_networks == expected_service_networks


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("url_overrides", "expected_reason"),
    [
        pytest.param(
            {_SERVICE_NAME_TO_URLS["GitHub"][0]: requests.HTTPError("403 Client Error: rate limit exceeded")},
            "HTTPError: 403 Client Error",
            id="error-status",
        ),
        pytest.param(
            {_SERVICE_NAME_TO_URLS["AWS"][0]: {"syncToken": "1"}}, "KeyError: 'prefixes'", id="changed-listing-layout"
        ),
        pytest.param(
            {_AZURE_SERVICE_TAGS_URL: None, _AZURE_DOWNLOAD_PAGE_URL: "<html>No link here</html>"},
            "Could not find the link to the Azure service tags file",
            id="moved-folder-and-changed-download-page",
        ),
        pytest.param(
            {
                _AZURE_SERVICE_TAGS_URL: requests.ConnectionError("Connection refused"),
                _AZURE_DOWNLOAD_PAGE_URL: requests.HTTPError("403 Client Error: Forbidden"),
            },
            "its download page could not be read either",
            id="download-server-and-page-unavailable",
        ),
        pytest.param({_SERVICE_NAME_TO_URLS["VPN"][0]: ""}, "holds no IPv4 ranges", id="empty-listing"),
    ],
)
def test_unavailable_listing_warning_names_the_cause(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    url_overrides: dict[str, dict | str | requests.RequestException | None],
    expected_reason: str,
) -> None:
    """However a listing breaks, the warning should say how, so that the source can be fixed."""
    for url, payload in url_overrides.items():
        if payload is None:
            del url_to_payload[url]
        else:
            url_to_payload[url] = payload

    with pytest.warns(ServiceNetworksWarning, match=expected_reason):
        fetch_service_networks()


@pytest.mark.ai_generated
def test_unavailable_listing_is_reported_once(
    url_to_payload: dict[str, dict | str | requests.RequestException], mocked_service_listings: list[str]
) -> None:
    """A broken listing should be tried and reported once per run, not again by every later fetch."""
    _break_listing(url_to_payload=url_to_payload, service_name="Azure")

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
    _break_listing(url_to_payload=url_to_payload, service_name="Azure")

    with pytest.raises(ServiceNetworksWarning, match="The published IP ranges of Azure are unavailable"):
        fetch_service_networks()


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "pass_cache_directory", [True, False], ids=["given-cache-directory", "default-cache-directory"]
)
def test_obtained_listings_are_saved(
    mocked_service_listings: list[str],
    tmp_path: pathlib.Path,
    default_listings_cache_directory: pathlib.Path,
    pass_cache_directory: bool,
) -> None:
    """Every listing obtained is saved in the cache directory, as the pairs the fetch returned."""
    cache_directory = tmp_path if pass_cache_directory else default_listings_cache_directory

    if pass_cache_directory:
        fetch_service_networks(tmp_path)
    else:
        fetch_service_networks()

    for service_name, expected_networks in _EXPECTED_SERVICE_NETWORKS.items():
        saved_listing_file_path = cache_directory / "service_networks" / f"{service_name}.json"
        saved_networks = [tuple(pair) for pair in json.loads(saved_listing_file_path.read_text())]
        assert saved_networks == expected_networks


@pytest.mark.ai_generated
@pytest.mark.parametrize("service_name", list(_SERVICE_NAME_TO_URLS.keys()))
def test_saved_listing_stands_in_for_an_unavailable_source(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    tmp_path: pathlib.Path,
    service_name: str,
) -> None:
    """A later run that cannot reach a source uses the copy saved by an earlier run, and says so."""
    fetch_service_networks(tmp_path)
    _start_new_run()
    _break_listing(url_to_payload=url_to_payload, service_name=service_name)

    with pytest.warns(ServiceNetworksWarning, match=f"{service_name} could not be obtained, so the copy saved"):
        service_networks = fetch_service_networks(tmp_path)

    assert service_networks == _EXPECTED_SERVICE_NETWORKS


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "saved_listing_text", [pytest.param(None, id="too-old"), pytest.param("not json", id="unreadable"), "[]"]
)
def test_saved_listing_is_not_used_when_too_old_or_unreadable(
    url_to_payload: dict[str, dict | str | requests.RequestException],
    mocked_service_listings: list[str],
    tmp_path: pathlib.Path,
    saved_listing_text: str | None,
) -> None:
    """A saved copy more than 30 days old, unreadable, or empty is not used, and the listing is unavailable."""
    fetch_service_networks(tmp_path)
    saved_listing_file_path = tmp_path / "service_networks" / "AWS.json"
    if saved_listing_text is None:
        thirty_one_days_ago = time.time() - 31 * 24 * 60 * 60
        os.utime(saved_listing_file_path, times=(thirty_one_days_ago, thirty_one_days_ago))
    else:
        saved_listing_file_path.write_text(saved_listing_text)
    _start_new_run()
    _break_listing(url_to_payload=url_to_payload, service_name="AWS")

    with pytest.warns(ServiceNetworksWarning, match="The published IP ranges of AWS are unavailable"):
        service_networks = fetch_service_networks(tmp_path)

    assert service_networks["AWS"] == []


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


@pytest.mark.remote
@pytest.mark.ai_generated
def test_azure_file_is_found_without_its_download_page_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    The newest Azure file should be found on the download server alone, without reading its download page.

    The download page is only a fallback, so a failure of the lookup by date would otherwise go unnoticed for as long
    as the page answers. The page sits behind bot protection that turns away CI machines at times, which is what the
    lookup by date exists to avoid.
    """
    ip_utils_module = s3_log_extraction.ip_utils._ip_utils
    request_published_listing = ip_utils_module._request_published_listing

    def _request_published_listing_but_not_the_download_page(url: str, /) -> requests.Response:
        if url == ip_utils_module._AZURE_DOWNLOAD_PAGE_URL:
            message = "The Azure service tags file was not found on the download server, so its download page was read."
            raise AssertionError(message)
        return request_published_listing(url)

    monkeypatch.setattr(
        ip_utils_module, "_request_published_listing", _request_published_listing_but_not_the_download_page
    )

    azure_service_tags = ip_utils_module._request_azure_service_tags()

    assert any(service_tag["name"] == "AzureCloud" for service_tag in azure_service_tags["values"])

"""Tests for excluding individually reviewed requesters from the view and requester counts."""

import pathlib

import pandas
import pytest

import s3_log_extraction
from s3_log_extraction.config import get_config, get_excluded_ips, set_excluded_ips, unset_excluded_ips
from s3_log_extraction.ip_utils import MappingRegionResolver, is_excluded_ip

# Every address below is drawn from the documentation ranges (TEST-NET-1, TEST-NET-2, and 2001:db8::/32)
_ACTOR_IP = "198.51.100.0"
_OTHER_IPS = [f"192.0.2.{index}" for index in range(10)]
_STREAMING = 0
_DOWNLOAD = 1


@pytest.fixture
def isolated_config(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the configuration file at a temporary location rather than the user's real one."""
    config_file_path = tmp_path / "config.yaml"
    monkeypatch.setattr("s3_log_extraction.config._config.S3_LOG_EXTRACTION_CONFIG_FILE_PATH", config_file_path)
    return config_file_path


def _write_asset(*, asset_directory: pathlib.Path, requests: list[tuple[str, int, str]]) -> None:
    """Write the line-aligned plaintext per-request files of one asset from ``(timestamp, download, ip)`` triplets."""
    asset_directory.mkdir(parents=True, exist_ok=True)
    (asset_directory / "timestamps.txt").write_text("\n".join(timestamp for timestamp, _, _ in requests) + "\n")
    (asset_directory / "download.txt").write_text("\n".join(str(download) for _, download, _ in requests) + "\n")
    (asset_directory / "bytes_sent.txt").write_text("\n".join(["1"] * len(requests)) + "\n")
    (asset_directory / "ips.txt").write_text("\n".join(ip for _, _, ip in requests) + "\n")


@pytest.mark.ai_generated
def test_excluded_ips_are_empty_when_unconfigured(isolated_config: pathlib.Path) -> None:
    """With nothing configured, no address is excluded."""
    assert get_excluded_ips() == frozenset()


@pytest.mark.ai_generated
def test_set_and_unset_excluded_ips(isolated_config: pathlib.Path) -> None:
    """Configured addresses are stored in canonical form, replace earlier ones, and can be cleared."""
    set_excluded_ips(["192.0.2.0"])
    set_excluded_ips([" 198.51.100.0 ", "2001:DB8::0001", "198.51.100.0"])

    assert get_excluded_ips() == frozenset({"198.51.100.0", "2001:db8::1"})
    assert get_config() == {"excluded_ips": ["198.51.100.0", "2001:db8::1"]}

    unset_excluded_ips()

    assert get_excluded_ips() == frozenset()
    assert get_config() == {}


@pytest.mark.ai_generated
def test_set_excluded_ips_with_no_addresses_clears_the_setting(isolated_config: pathlib.Path) -> None:
    """Setting an empty list is the same as unsetting, so no empty key lingers in the configuration."""
    set_excluded_ips(["198.51.100.0"])
    set_excluded_ips([])

    assert get_config() == {}


@pytest.mark.ai_generated
@pytest.mark.parametrize("invalid_entry", ["198.51.100.0/24", "not-an-address", ""])
def test_set_excluded_ips_rejects_anything_but_single_addresses(
    isolated_config: pathlib.Path, invalid_entry: str
) -> None:
    """A network or a malformed entry is refused and leaves the configured list as it was."""
    set_excluded_ips(["192.0.2.0"])

    with pytest.raises(ValueError, match="is not a single valid IP address"):
        set_excluded_ips(["198.51.100.0", invalid_entry])

    assert get_excluded_ips() == frozenset({"192.0.2.0"})


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("ip", "excluded_ips", "expected"),
    [
        # An exact listed address is excluded
        ("198.51.100.0", frozenset({"198.51.100.0"}), True),
        # A neighbouring address in the same network is NOT excluded
        ("198.51.100.1", frozenset({"198.51.100.0"}), False),
        # An empty list excludes nothing
        ("198.51.100.0", frozenset(), False),
    ],
)
def test_is_excluded_ip(ip: str, excluded_ips: frozenset[str], expected: bool) -> None:
    """Only exact listed addresses are excluded."""
    assert is_excluded_ip(ip=ip, excluded_ips=excluded_ips) == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("configured_ips", "unset_afterwards", "excluded_ips", "expected_is_actor_excluded"),
    [
        # Nothing configured, so every published number is as it was
        (None, False, None, False),
        # Configured and then reset, so every published number is as it was
        ([_ACTOR_IP], True, None, False),
        # An explicit empty argument overrides the configuration
        ([_ACTOR_IP], False, (), False),
        # The configured list is applied by default
        ([_ACTOR_IP], False, None, True),
        # An explicit argument is applied with nothing configured
        (None, False, [_ACTOR_IP], True),
    ],
)
def test_summaries_exclude_listed_requesters_from_views_and_requesters_only(
    isolated_config: pathlib.Path,
    tmp_path: pathlib.Path,
    configured_ips: list[str] | None,
    unset_afterwards: bool,
    excluded_ips: list[str] | tuple[()] | None,
    expected_is_actor_excluded: bool,
) -> None:
    """
    An excluded address leaves the view and requester counts, but not bytes sent, requests, or downloads.

    The actor carries a plain geographic label, as the reviewed actor does, so no label-based exclusion
    already removes it.
    """
    extraction_directory = tmp_path / "extraction" / "ds001"
    _write_asset(
        asset_directory=extraction_directory / "first.nwb",
        requests=[
            *[("250101000000", _STREAMING, ip) for ip in _OTHER_IPS],
            ("250101000000", _STREAMING, _ACTOR_IP),
            ("250102000000", _STREAMING, _ACTOR_IP),
            ("250103000000", _STREAMING, _ACTOR_IP),
        ],
    )
    _write_asset(
        asset_directory=extraction_directory / "second.nwb",
        requests=[
            ("250101000000", _STREAMING, _ACTOR_IP),
            ("250102000000", _STREAMING, _ACTOR_IP),
            ("250103000000", _DOWNLOAD, _ACTOR_IP),
        ],
    )
    region_resolver = MappingRegionResolver(
        {_ACTOR_IP: "USA/NH"} | {ip: f"USA/Subdivision {index % 5}" for index, ip in enumerate(_OTHER_IPS)}
    )
    if configured_ips is not None:
        set_excluded_ips(configured_ips)
    if unset_afterwards:
        unset_excluded_ips()

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=tmp_path,
        use_encryption=False,
        region_disclosure_threshold=1,
        region_resolver=region_resolver,
        excluded_ips=excluded_ips,
    )

    summary_directory = tmp_path / "summaries"
    expected_number_of_views = 10 if expected_is_actor_excluded else 15
    expected_number_of_requesters = "10" if expected_is_actor_excluded else "11"
    for summary_file_name in ("by_asset.tsv", "by_day.tsv", "by_region.tsv"):
        summary = pandas.read_table(filepath_or_buffer=summary_directory / "ds001" / summary_file_name)
        assert int(summary["number_of_views"].sum()) == expected_number_of_views, summary_file_name
        assert int(summary["bytes_sent"].sum()) == 16, summary_file_name
        assert int(summary["number_of_requests"].sum()) == 16, summary_file_name
        assert int(summary["number_of_downloads"].sum()) == 1, summary_file_name
    assert (summary_directory / "ds001" / "requester_count.tsv").read_text() == expected_number_of_requesters
    assert (summary_directory / "archive" / "requester_count.tsv").read_text() == expected_number_of_requesters

"""Tests for excluding individually reviewed requesters from every summary."""

import pathlib

import pandas
import pytest

import s3_log_extraction
from s3_log_extraction.config import get_excluded_ips
from s3_log_extraction.ip_utils import MappingRegionResolver, is_excluded_ip

# Every address below is drawn from the documentation ranges (TEST-NET-1, TEST-NET-2, and 2001:db8::/32)
_ACTOR_IP = "198.51.100.0"
_OTHER_IPS = [f"192.0.2.{index}" for index in range(10)]
_STREAMING = 0
_DOWNLOAD = 1


@pytest.fixture
def excluded_ips_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the excluded IPs file at a temporary location rather than the user's real one, without creating it."""
    excluded_ips_file_path = tmp_path / "excluded_ips.txt"
    monkeypatch.setattr("s3_log_extraction.config._config.EXCLUDED_IPS_FILE_PATH", excluded_ips_file_path)
    return excluded_ips_file_path


def _write_asset(*, asset_directory: pathlib.Path, requests: list[tuple[str, int, str]]) -> None:
    """Write the line-aligned plaintext per-request files of one asset from ``(timestamp, download, ip)`` triplets."""
    asset_directory.mkdir(parents=True, exist_ok=True)
    (asset_directory / "timestamps.txt").write_text("\n".join(timestamp for timestamp, _, _ in requests) + "\n")
    (asset_directory / "download.txt").write_text("\n".join(str(download) for _, download, _ in requests) + "\n")
    (asset_directory / "bytes_sent.txt").write_text("\n".join(["1"] * len(requests)) + "\n")
    (asset_directory / "ips.txt").write_text("\n".join(ip for _, _, ip in requests) + "\n")


@pytest.mark.ai_generated
@pytest.mark.parametrize("file_content", [None, "", "\n\n# A comment and nothing else\n"])
def test_excluded_ips_are_empty_without_any_listed_address(
    excluded_ips_file_path: pathlib.Path, file_content: str | None
) -> None:
    """An absent file, an empty file, and a file of only comments all exclude nothing."""
    if file_content is not None:
        excluded_ips_file_path.write_text(file_content)

    assert get_excluded_ips() == frozenset()


@pytest.mark.ai_generated
def test_excluded_ips_are_read_from_the_file(excluded_ips_file_path: pathlib.Path) -> None:
    """Comments and blank lines are skipped, and each address is read in canonical form."""
    excluded_ips_file_path.write_text(
        "# Reviewed mirror\n"
        "\n"
        "  198.51.100.0  \n"
        "2001:DB8::0001  # Same actor, second address\n"
        "198.51.100.0\n"
    )

    assert get_excluded_ips() == frozenset({"198.51.100.0", "2001:db8::1"})


@pytest.mark.ai_generated
@pytest.mark.parametrize("invalid_entry", ["198.51.100.0/24", "not-an-address", "192.0.2.0 198.51.100.0"])
def test_excluded_ips_reject_anything_but_single_addresses(
    excluded_ips_file_path: pathlib.Path, invalid_entry: str
) -> None:
    """A network or a malformed line is refused, naming the line, rather than silently matching nothing."""
    excluded_ips_file_path.write_text(f"192.0.2.0\n{invalid_entry}\n")

    with pytest.raises(ValueError, match="Line 2 of"):
        get_excluded_ips()


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
    ("file_content", "excluded_ips", "expected_is_actor_excluded"),
    [
        # No file, so every published number is as it was
        (None, None, False),
        # A file emptied of addresses, so every published number is as it was
        ("# Nothing excluded at the moment\n", None, False),
        # An explicit empty argument overrides the file
        (f"{_ACTOR_IP}\n", (), False),
        # The listed addresses are applied by default
        (f"{_ACTOR_IP}\n", None, True),
        # An explicit argument is applied with no file
        (None, [_ACTOR_IP], True),
    ],
)
def test_summaries_exclude_listed_requesters_from_every_summary(
    excluded_ips_file_path: pathlib.Path,
    tmp_path: pathlib.Path,
    file_content: str | None,
    excluded_ips: list[str] | tuple[()] | None,
    expected_is_actor_excluded: bool,
) -> None:
    """
    An excluded address leaves every summary: bytes sent, requests, downloads, views, and requester counts.

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
    if file_content is not None:
        excluded_ips_file_path.write_text(file_content)

    s3_log_extraction.summarize.generate_summaries(
        cache_directory=tmp_path,
        use_encryption=False,
        region_disclosure_threshold=1,
        region_resolver=region_resolver,
        excluded_ips=excluded_ips,
    )

    summary_directory = tmp_path / "summaries"
    expected_number_of_views = 10 if expected_is_actor_excluded else 15
    expected_number_of_requests = 10 if expected_is_actor_excluded else 16
    expected_number_of_downloads = 0 if expected_is_actor_excluded else 1
    expected_number_of_requesters = "10" if expected_is_actor_excluded else "11"
    for summary_file_name in ("by_asset.tsv", "by_day.tsv", "by_region.tsv"):
        summary = pandas.read_table(filepath_or_buffer=summary_directory / "ds001" / summary_file_name)
        assert int(summary["number_of_views"].sum()) == expected_number_of_views, summary_file_name
        assert int(summary["bytes_sent"].sum()) == expected_number_of_requests, summary_file_name
        assert int(summary["number_of_requests"].sum()) == expected_number_of_requests, summary_file_name
        assert int(summary["number_of_downloads"].sum()) == expected_number_of_downloads, summary_file_name
    by_region = pandas.read_table(filepath_or_buffer=summary_directory / "ds001" / "by_region.tsv")
    assert ("USA/NH" in by_region["region"].tolist()) == (not expected_is_actor_excluded)
    assert (summary_directory / "ds001" / "requester_count.tsv").read_text() == expected_number_of_requesters
    assert (summary_directory / "archive" / "requester_count.tsv").read_text() == expected_number_of_requesters

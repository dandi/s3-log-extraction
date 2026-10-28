"""Tests for excluding individually reviewed requesters from every summary."""

import pathlib
import shutil
import subprocess

import click.testing
import pandas
import pytest
from conftest import read_extracted_ips

import s3_log_extraction
from s3_log_extraction.config import EXCLUDED_IPS_ENVIRONMENT_VARIABLE, get_excluded_ips
from s3_log_extraction.ip_utils import MappingRegionResolver, is_excluded_ip

# Every address below is drawn from the documentation ranges (TEST-NET-1, TEST-NET-2, and 2001:db8::/32)
_ACTOR_IP = "198.51.100.0"
_OTHER_IPS = [f"192.0.2.{index}" for index in range(10)]
_STREAMING = 0
_DOWNLOAD = 1


_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_KEY = EXCLUDED_IPS_ENVIRONMENT_VARIABLE
_MULTI_LINE_DOTENV_CONTENT = (
    "# Reviewed requesters\n"
    f'{_KEY}="\n'
    "198.51.100.0  # Reviewed mirror\n"
    "\n"
    "   # A comment on a line of its own\n"
    "2001:DB8::0001 # Same actor, second address\n"
    "192.0.2.1, 192.0.2.2\n"
    '"\n'
)
_MULTI_LINE_EXPECTED_IPS = frozenset({"198.51.100.0", "2001:db8::1", "192.0.2.1", "192.0.2.2"})


@pytest.fixture
def dotenv_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Point the dotenv file at a temporary location rather than the user's real one, without creating it.

    The environment variable is cleared and the legacy file is pointed at a temporary location as well, so that
    nothing outside the test contributes an address.
    """
    dotenv_file_path = tmp_path / ".env"
    monkeypatch.setattr("s3_log_extraction.config._config.DOTENV_FILE_PATH", dotenv_file_path)
    monkeypatch.setattr(
        "s3_log_extraction.config._config._LEGACY_EXCLUDED_IPS_FILE_PATH", tmp_path / "excluded_ips.txt"
    )
    monkeypatch.delenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, raising=False)
    return dotenv_file_path


def _write_asset(*, asset_directory: pathlib.Path, requests: list[tuple[str, int, str]]) -> None:
    """Write the line-aligned plaintext per-request files of one asset from ``(timestamp, download, ip)`` triplets."""
    asset_directory.mkdir(parents=True, exist_ok=True)
    (asset_directory / "timestamps.txt").write_text("\n".join(timestamp for timestamp, _, _ in requests) + "\n")
    (asset_directory / "download.txt").write_text("\n".join(str(download) for _, download, _ in requests) + "\n")
    (asset_directory / "bytes_sent.txt").write_text("\n".join(["1"] * len(requests)) + "\n")
    (asset_directory / "ips.txt").write_text("\n".join(ip for _, _, ip in requests) + "\n")


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "dotenv_content",
    [None, "", "\n# A comment and nothing else\n", f"{_KEY}=\n", "UNRELATED_VARIABLE=192.0.2.1\n"],
)
def test_excluded_ips_are_empty_without_any_listed_address(
    dotenv_file_path: pathlib.Path, dotenv_content: str | None
) -> None:
    """An absent, empty or unrelated dotenv file, and an empty entry, all exclude nothing."""
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)

    assert get_excluded_ips() == frozenset()


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("environment_value", "dotenv_content", "extra_ips", "expected"),
    [
        # The dotenv entry is read, with full-line comments, and each address is canonicalized
        (
            None,
            f"# Reviewed mirror and its second address\n{_KEY}=198.51.100.0,2001:DB8::0001,198.51.100.0\n",
            None,
            {"198.51.100.0", "2001:db8::1"},
        ),
        # A multi-line value holds one address per line, each optionally followed by a comment
        (None, _MULTI_LINE_DOTENV_CONTENT, None, set(_MULTI_LINE_EXPECTED_IPS)),
        (None, _MULTI_LINE_DOTENV_CONTENT, ["203.0.113.0"], {*_MULTI_LINE_EXPECTED_IPS, "203.0.113.0"}),
        # Whitespace also separates addresses, as when the variable is set in Python or a quoted shell value
        ("192.0.2.1, 2001:DB8::0001\n192.0.2.2", None, None, {"192.0.2.1", "192.0.2.2", "2001:db8::1"}),
        # A variable set in the environment takes precedence over the dotenv file, even when empty
        ("192.0.2.3", f"{_KEY}=192.0.2.1\n", None, {"192.0.2.3"}),
        ("", f"{_KEY}=192.0.2.1\n", None, set()),
        # Extra addresses add to the dotenv entry or the environment variable rather than replacing it
        (None, f"{_KEY}=192.0.2.1\n", ["192.0.2.2", " 192.0.2.3 ", ""], {"192.0.2.1", "192.0.2.2", "192.0.2.3"}),
        ("192.0.2.1", None, ["192.0.2.2"], {"192.0.2.1", "192.0.2.2"}),
        (None, None, ["192.0.2.2"], {"192.0.2.2"}),
    ],
)
def test_excluded_ips_combine_the_dotenv_entry_and_extra_addresses(
    monkeypatch: pytest.MonkeyPatch,
    dotenv_file_path: pathlib.Path,
    environment_value: str | None,
    dotenv_content: str | None,
    extra_ips: list[str] | None,
    expected: set[str],
) -> None:
    """The environment variable, or the dotenv entry when it is not set, is united with any extra addresses."""
    if environment_value is not None:
        monkeypatch.setenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, environment_value)
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)

    assert get_excluded_ips(extra_ips) == frozenset(expected)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dotenv_content", "expected"),
    [
        pytest.param(_MULTI_LINE_DOTENV_CONTENT, _MULTI_LINE_EXPECTED_IPS, id="multi_line"),
        pytest.param(
            f"# Reviewed requesters\n{_KEY}=192.0.2.1,198.51.100.0\n",
            frozenset({"192.0.2.1", "198.51.100.0"}),
            id="single_line",
        ),
    ],
)
def test_dotenv_entry_parses_the_same_when_a_shell_exports_it(
    monkeypatch: pytest.MonkeyPatch, dotenv_file_path: pathlib.Path, dotenv_content: str, expected: frozenset[str]
) -> None:
    """
    The value a shell exports by sourcing the dotenv file parses exactly as the file read by python-dotenv does.

    A sourcing shell keeps the newlines of a double-quoted value and leaves its ``#`` text literal, so the
    comments reach the parser in the environment variable too.
    """
    dotenv_file_path.write_text(dotenv_content)
    from_dotenv_file = get_excluded_ips()

    # The command is a fixed string around a temporary path, so nothing untrusted reaches the shell
    result = subprocess.run(  # noqa: S603
        args=[shutil.which("bash"), "-c", f'set -a; . "{dotenv_file_path}"; set +a; printf "%s" "${_KEY}"'],
        capture_output=True,
        text=True,
        check=True,
    )
    dotenv_file_path.unlink()
    monkeypatch.setenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, result.stdout)
    from_environment = get_excluded_ips()

    assert from_dotenv_file == expected
    assert from_environment == expected


@pytest.mark.ai_generated
def test_excluded_ips_refuse_to_run_while_the_legacy_file_exists(dotenv_file_path: pathlib.Path) -> None:
    """A leftover 1.11.10 file stops the run, rather than silently counting its requesters again."""
    (dotenv_file_path.parent / "excluded_ips.txt").write_text("192.0.2.1\n")

    with pytest.raises(ValueError, match="is no longer read") as exception_info:
        get_excluded_ips()

    assert f'{_KEY}="\n192.0.2.1  # Why it is excluded\n' in str(exception_info.value)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dotenv_content", "extra_ips", "expected_message"),
    [
        (f"{_KEY}=192.0.2.1|192.0.2.2\n", None, _KEY),
        (f"{_KEY}=192.0.2.0/24\n", None, _KEY),
        (f"{_KEY}=not-an-address\n", None, _KEY),
        (None, ["192.0.2.*"], "extra excluded IPs"),
    ],
)
def test_excluded_ips_reject_patterns_and_networks(
    dotenv_file_path: pathlib.Path,
    dotenv_content: str | None,
    extra_ips: list[str] | None,
    expected_message: str,
) -> None:
    """A regex, a wildcard or a network is refused, naming its source, rather than silently matching nothing."""
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)

    with pytest.raises(ValueError, match=expected_message):
        get_excluded_ips(extra_ips)


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize(
    ("dotenv_content", "excluded_ips", "expected_dropped_ips"),
    [
        # Nothing listed, so nothing is dropped
        (None, None, set()),
        # The listed addresses are dropped by default
        (f"{_KEY}=192.0.2.1\n", None, {"192.0.2.1"}),
        # An explicit argument replaces the default
        (f"{_KEY}=192.0.2.1\n", ["198.51.100.0"], {"198.51.100.0"}),
        (f"{_KEY}=192.0.2.1\n", [], set()),
    ],
)
def test_extraction_drops_excluded_ips(
    dotenv_file_path: pathlib.Path,
    tmp_path: pathlib.Path,
    workers: int,
    dotenv_content: str | None,
    excluded_ips: list[str] | None,
    expected_dropped_ips: set[str],
) -> None:
    """Requests from an excluded address never enter the extraction cache, while every other request does."""
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()

    extractor = s3_log_extraction.extractors.S3LogAccessExtractor(
        cache_directory=cache_directory, use_encryption=False, excluded_ips=excluded_ips
    )
    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=workers)

    extracted_ips = set(read_extracted_ips(cache_directory / "extraction", use_encryption=False))
    assert extracted_ips & expected_dropped_ips == set()
    assert extracted_ips | expected_dropped_ips == {
        "192.0.2.0",
        "192.0.2.1",
        "192.0.2.2",
        "198.51.100.0",
        "198.51.100.1",
        "203.0.113.0",
        "203.0.113.1",
    }


@pytest.mark.ai_generated
def test_cli_extract_drops_the_dotenv_entry_and_the_extra_addresses(
    dotenv_file_path: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The ``--exclude`` addresses are dropped together with those of the dotenv entry."""
    dotenv_file_path.write_text(f"{_KEY}=192.0.2.1,192.0.2.2\n")
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()

    result = click.testing.CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            str(_EXAMPLE_LOGS_DIRECTORY),
            "--cache",
            str(cache_directory),
            "--workers",
            "1",
            "--encryption",
            "false",
            "--exclude",
            "198.51.100.0,198.51.100.1",
        ],
    )

    assert result.exit_code == 0, result.output
    extracted_ips = set(read_extracted_ips(cache_directory / "extraction", use_encryption=False))
    assert extracted_ips == {"192.0.2.0", "203.0.113.0", "203.0.113.1"}


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("ip", "excluded_ips", "expected"),
    [
        # An exact listed address is excluded
        ("198.51.100.0", frozenset({"198.51.100.0"}), True),
        # A neighboring address in the same network is NOT excluded
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
    ("dotenv_content", "excluded_ips", "expected_is_actor_excluded"),
    [
        # No file, so every published number is as it was
        (None, None, False),
        # An entry emptied of addresses, so every published number is as it was
        (f"# Nothing excluded at the moment\n{_KEY}=\n", None, False),
        # An explicit empty argument overrides the file
        (f"{_KEY}={_ACTOR_IP}\n", (), False),
        # The listed addresses are applied by default
        (f"{_KEY}={_ACTOR_IP}\n", None, True),
        # An explicit argument is applied with no file
        (None, [_ACTOR_IP], True),
    ],
)
def test_summaries_exclude_listed_requesters_from_every_summary(
    dotenv_file_path: pathlib.Path,
    tmp_path: pathlib.Path,
    dotenv_content: str | None,
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
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)

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

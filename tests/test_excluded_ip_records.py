"""Tests for scanning the extraction cache for records made by an excluded IP address."""

import pathlib
import secrets

import click.testing
import pytest

import s3_log_extraction
from s3_log_extraction.config import EXCLUDED_IPS_ENVIRONMENT_VARIABLE
from s3_log_extraction.ip_utils import find_excluded_ip_records
from s3_log_extraction.utils import write_text_to_file

# Every address below is drawn from the documentation ranges (TEST-NET-1, TEST-NET-2, TEST-NET-3, and 2001:db8::/32)
_ACTOR_IP = "198.51.100.0"
_SECOND_ACTOR_IP = "2001:db8::1"
_OTHER_IP = "192.0.2.0"
_ANOTHER_IP = "203.0.113.0"


@pytest.fixture
def dotenv_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the dotenv file at a temporary location and clear the variable, so that nothing outside the test counts."""
    dotenv_file_path = tmp_path / ".env"
    monkeypatch.setattr("s3_log_extraction.config._config.DOTENV_FILE_PATH", dotenv_file_path)
    monkeypatch.setattr(
        "s3_log_extraction.config._config._LEGACY_EXCLUDED_IPS_FILE_PATH", tmp_path / "excluded_ips.txt"
    )
    monkeypatch.delenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, raising=False)
    return dotenv_file_path


def _write_object_key(*, object_key_directory: pathlib.Path, ips: list[str], use_encryption: bool = False) -> None:
    """Write the line-aligned per-request files of one object key, with one request per address."""
    object_key_directory.mkdir(parents=True, exist_ok=True)
    for file_name in ("timestamps.txt", "bytes_sent.txt", "download.txt"):
        (object_key_directory / file_name).write_text("".join("1\n" for _ in ips))
    write_text_to_file(
        file_path=object_key_directory / "ips.txt", text="".join(f"{ip}\n" for ip in ips), use_encryption=use_encryption
    )


def _write_cache(*, cache_directory: pathlib.Path, use_encryption: bool = False) -> None:
    """Write an extraction cache in which two object keys hold records of the actors and two do not."""
    extraction_directory = cache_directory / "extraction"
    _write_object_key(
        object_key_directory=extraction_directory / "ds000001" / "sub-01" / "data.nii.gz",
        ips=[_OTHER_IP, _ACTOR_IP, _ACTOR_IP, _SECOND_ACTOR_IP],
        use_encryption=use_encryption,
    )
    _write_object_key(
        object_key_directory=extraction_directory / "ds000001" / "participants.tsv",
        ips=[_ANOTHER_IP, _OTHER_IP],
        use_encryption=use_encryption,
    )
    _write_object_key(
        object_key_directory=extraction_directory / "ds000002" / "dataset_description.json",
        ips=[_ACTOR_IP],
        use_encryption=use_encryption,
    )
    _write_object_key(
        object_key_directory=extraction_directory / "ds000002" / "empty.txt", ips=[], use_encryption=use_encryption
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
@pytest.mark.parametrize(
    ("excluded_ips", "expected_records"),
    [
        (
            [_ACTOR_IP, _SECOND_ACTOR_IP],
            {
                "ds000001/sub-01/data.nii.gz": {_ACTOR_IP: 2, _SECOND_ACTOR_IP: 1},
                "ds000002/dataset_description.json": {_ACTOR_IP: 1},
            },
        ),
        ([_SECOND_ACTOR_IP], {"ds000001/sub-01/data.nii.gz": {_SECOND_ACTOR_IP: 1}}),
        # An address with no record in the cache, and no address at all, both find nothing
        (["198.51.100.1"], {}),
        ([], {}),
    ],
)
def test_find_excluded_ip_records(
    tmp_path: pathlib.Path, workers: int, excluded_ips: list[str], expected_records: dict[str, dict[str, int]]
) -> None:
    """Each object key with records of an excluded address is reported with its count per address, sorted by key."""
    _write_cache(cache_directory=tmp_path)

    records = find_excluded_ip_records(
        cache_directory=tmp_path, excluded_ips=excluded_ips, use_encryption=False, workers=workers
    )

    assert records == expected_records
    assert list(records) == sorted(expected_records)


@pytest.mark.ai_generated
def test_find_excluded_ip_records_decrypts_the_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """The ``ips.txt`` files of an encrypted cache are decrypted before they are scanned."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", secrets.token_urlsafe(32))
    _write_cache(cache_directory=tmp_path, use_encryption=True)

    records = find_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], workers=1)

    assert records == {
        "ds000001/sub-01/data.nii.gz": {_ACTOR_IP: 2},
        "ds000002/dataset_description.json": {_ACTOR_IP: 1},
    }


@pytest.mark.ai_generated
def test_find_excluded_ip_records_defaults_to_the_configured_addresses(
    dotenv_file_path: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Without explicit addresses, those of the configured sources are looked for."""
    dotenv_file_path.write_text(f"{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}={_SECOND_ACTOR_IP}\n")
    _write_cache(cache_directory=tmp_path)

    records = find_excluded_ip_records(cache_directory=tmp_path, use_encryption=False, workers=1)

    assert records == {"ds000001/sub-01/data.nii.gz": {_SECOND_ACTOR_IP: 1}}


@pytest.mark.ai_generated
def test_find_excluded_ip_records_requires_an_extraction_directory(tmp_path: pathlib.Path) -> None:
    """A cache without an extraction directory is refused rather than reported as clean."""
    with pytest.raises(FileNotFoundError, match="No extraction directory"):
        find_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("object_key", "file_name", "file_content", "expected_message"),
    [
        # A missing file is refused whether or not the object key holds a record of an excluded address
        ("ds000001/participants.tsv", "download.txt", None, "incomplete: download.txt not found"),
        ("ds000001/sub-01/data.nii.gz", "ips.txt", None, "incomplete: ips.txt not found"),
        # A misaligned object key holding a record of an excluded address is refused
        ("ds000001/sub-01/data.nii.gz", "bytes_sent.txt", "1\n", "not line-aligned"),
    ],
)
def test_find_excluded_ip_records_rejects_a_corrupted_cache(
    tmp_path: pathlib.Path, object_key: str, file_name: str, file_content: str | None, expected_message: str
) -> None:
    """Object keys whose files are missing or misaligned are refused rather than silently skipped."""
    _write_cache(cache_directory=tmp_path)
    file_path = tmp_path / "extraction" / object_key / file_name
    if file_content is None:
        file_path.unlink()
    else:
        file_path.write_text(file_content)

    with pytest.raises(RuntimeError, match=expected_message):
        find_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1)


@pytest.mark.ai_generated
def test_cli_excluded_scan_fails_on_found_records(
    monkeypatch: pytest.MonkeyPatch, dotenv_file_path: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Found records are listed per object key and fail the command, without printing the addresses."""
    monkeypatch.setenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, f"{_ACTOR_IP},198.51.100.1")
    _write_cache(cache_directory=tmp_path)

    result = click.testing.CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "excluded",
            "scan",
            "--cache",
            str(tmp_path),
            "--encryption",
            "false",
            "--workers",
            "1",
            "--exclude",
            _SECOND_ACTOR_IP,
        ],
    )

    assert result.exit_code == 1
    assert "ds000001/sub-01/data.nii.gz: 3 records" in result.output
    assert "ds000002/dataset_description.json: 1 records" in result.output
    assert "Found 4 records of 2 excluded IP addresses across 2 object keys" in result.output
    assert _ACTOR_IP not in result.output
    assert _SECOND_ACTOR_IP not in result.output


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("file_content", "expected_output"),
    [
        (
            f"{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}=198.51.100.1\n",
            "No records of the 1 excluded IP addresses were found.",
        ),
        (None, "No excluded IP addresses are configured"),
    ],
)
def test_cli_excluded_scan_passes_without_found_records(
    dotenv_file_path: pathlib.Path, tmp_path: pathlib.Path, file_content: str | None, expected_output: str
) -> None:
    """A cache without records of an excluded address, or no excluded address at all, passes with a short note."""
    if file_content is not None:
        dotenv_file_path.write_text(file_content)
    _write_cache(cache_directory=tmp_path)

    result = click.testing.CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        ["excluded", "scan", "--cache", str(tmp_path), "--encryption", "false", "--workers", "1"],
    )

    assert result.exit_code == 0, result.output
    assert expected_output in result.output

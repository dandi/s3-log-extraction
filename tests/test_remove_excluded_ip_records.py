"""Tests for removing the records made by an excluded IP address from the extraction cache."""

import pathlib
import secrets

import click.testing
import pytest

import s3_log_extraction
from s3_log_extraction.config import EXCLUDED_IPS_ENVIRONMENT_VARIABLE
from s3_log_extraction.ip_utils import find_excluded_ip_records, remove_excluded_ip_records
from s3_log_extraction.utils import read_text_from_file, write_text_to_file

# Every address below is drawn from the documentation ranges (TEST-NET-1, TEST-NET-2, TEST-NET-3, and 2001:db8::/32)
_ACTOR_IP = "198.51.100.0"
_SECOND_ACTOR_IP = "2001:db8::1"
_OTHER_IP = "192.0.2.0"
_ANOTHER_IP = "203.0.113.0"

_DATA_KEY = "ds000001/sub-01/data.nii.gz"
_PARTICIPANTS_KEY = "ds000001/participants.tsv"
_DESCRIPTION_KEY = "ds000002/dataset_description.json"
_EMPTY_KEY = "ds000002/empty.txt"
_ONLY_ACTOR_KEY = "ds000003/sub-01/only.json"


@pytest.fixture
def dotenv_file_path(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point every source of excluded IPs at a temporary location, so that none outside the test contributes."""
    dotenv_file_path = tmp_path / ".env"
    monkeypatch.setattr("s3_log_extraction.config._config.DOTENV_FILE_PATH", dotenv_file_path)
    monkeypatch.setattr(
        "s3_log_extraction.config._config._LEGACY_EXCLUDED_IPS_FILE_PATH", tmp_path / "excluded_ips.txt"
    )
    monkeypatch.delenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, raising=False)
    return dotenv_file_path


def _write_object_key(*, object_key_directory: pathlib.Path, ips: list[str], use_encryption: bool = False) -> None:
    """
    Write the line-aligned per-request files of one object key, with one request per address.

    Every file numbers its lines differently, so that a misaligned removal shows up as a mismatch.
    """
    object_key_directory.mkdir(parents=True, exist_ok=True)
    (object_key_directory / "timestamps.txt").write_text(
        "".join(f"2020010100000{index}\n" for index in range(len(ips)))
    )
    (object_key_directory / "bytes_sent.txt").write_text("".join(f"{100 + index}\n" for index in range(len(ips))))
    (object_key_directory / "download.txt").write_text("".join(f"{index % 2}\n" for index in range(len(ips))))
    write_text_to_file(
        file_path=object_key_directory / "ips.txt", text="".join(f"{ip}\n" for ip in ips), use_encryption=use_encryption
    )


def _write_cache(*, cache_directory: pathlib.Path, use_encryption: bool = False) -> None:
    """Write an extraction cache in which three object keys hold records of the actors and two do not."""
    object_key_ips = {
        _DATA_KEY: [_OTHER_IP, _ACTOR_IP, _ACTOR_IP, _SECOND_ACTOR_IP, _ANOTHER_IP],
        _PARTICIPANTS_KEY: [_ANOTHER_IP, _OTHER_IP],
        _DESCRIPTION_KEY: [_ACTOR_IP],
        _EMPTY_KEY: [],
        _ONLY_ACTOR_KEY: [_SECOND_ACTOR_IP, _ACTOR_IP],
    }
    for object_key, ips in object_key_ips.items():
        _write_object_key(
            object_key_directory=cache_directory / "extraction" / object_key, ips=ips, use_encryption=use_encryption
        )


def _read_object_key(*, object_key_directory: pathlib.Path, use_encryption: bool = False) -> dict[str, list[str]]:
    """Read the lines of every per-request file of one object key."""
    lines_per_file = {
        file_name: (object_key_directory / file_name).read_text().splitlines()
        for file_name in ("timestamps.txt", "bytes_sent.txt", "download.txt")
    }
    lines_per_file["ips.txt"] = read_text_from_file(
        file_path=object_key_directory / "ips.txt", use_encryption=use_encryption
    ).splitlines()
    return lines_per_file


# The lines that are left of the data object key once both actors are removed, which are its first and last
_EXPECTED_DATA_LINES = {
    "timestamps.txt": ["20200101000000", "20200101000004"],
    "bytes_sent.txt": ["100", "104"],
    "download.txt": ["0", "0"],
    "ips.txt": [_OTHER_IP, _ANOTHER_IP],
}


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
def test_remove_excluded_ip_records(tmp_path: pathlib.Path, workers: int) -> None:
    """Matching lines leave every file of an object key together, and emptied object keys leave the cache."""
    _write_cache(cache_directory=tmp_path)
    extraction_directory = tmp_path / "extraction"
    untouched_lines = _read_object_key(object_key_directory=extraction_directory / _PARTICIPANTS_KEY)

    records = remove_excluded_ip_records(
        cache_directory=tmp_path, excluded_ips=[_ACTOR_IP, _SECOND_ACTOR_IP], use_encryption=False, workers=workers
    )

    assert records == {
        _DATA_KEY: {_ACTOR_IP: 2, _SECOND_ACTOR_IP: 1},
        _DESCRIPTION_KEY: {_ACTOR_IP: 1},
        _ONLY_ACTOR_KEY: {_ACTOR_IP: 1, _SECOND_ACTOR_IP: 1},
    }
    assert list(records) == sorted(records)
    assert _read_object_key(object_key_directory=extraction_directory / _DATA_KEY) == _EXPECTED_DATA_LINES
    assert _read_object_key(object_key_directory=extraction_directory / _PARTICIPANTS_KEY) == untouched_lines
    assert not (extraction_directory / _DESCRIPTION_KEY).exists()
    assert (extraction_directory / _EMPTY_KEY).is_dir()
    assert not (extraction_directory / "ds000003").exists()
    assert not list(extraction_directory.rglob(pattern="*.tmp"))

    remaining_records = find_excluded_ip_records(
        cache_directory=tmp_path, excluded_ips=[_ACTOR_IP, _SECOND_ACTOR_IP], use_encryption=False, workers=1
    )
    assert remaining_records == {}


@pytest.mark.ai_generated
def test_remove_excluded_ip_records_keeps_the_cache_encrypted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """The rewritten ``ips.txt`` files of an encrypted cache are encrypted again."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", secrets.token_urlsafe(32))
    _write_cache(cache_directory=tmp_path, use_encryption=True)
    data_directory = tmp_path / "extraction" / _DATA_KEY

    records = remove_excluded_ip_records(
        cache_directory=tmp_path, excluded_ips=[_ACTOR_IP, _SECOND_ACTOR_IP], workers=1
    )

    assert records[_DATA_KEY] == {_ACTOR_IP: 2, _SECOND_ACTOR_IP: 1}
    assert _OTHER_IP.encode() not in (data_directory / "ips.txt").read_bytes()
    assert _read_object_key(object_key_directory=data_directory, use_encryption=True) == _EXPECTED_DATA_LINES


@pytest.mark.ai_generated
def test_remove_excluded_ip_records_defaults_to_the_configured_addresses(
    dotenv_file_path: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Without explicit addresses, those of the configured sources are removed."""
    dotenv_file_path.write_text(f"{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}={_SECOND_ACTOR_IP}\n")
    _write_cache(cache_directory=tmp_path)

    records = remove_excluded_ip_records(cache_directory=tmp_path, use_encryption=False, workers=1)

    assert records == {_DATA_KEY: {_SECOND_ACTOR_IP: 1}, _ONLY_ACTOR_KEY: {_SECOND_ACTOR_IP: 1}}


@pytest.mark.ai_generated
def test_remove_excluded_ip_records_without_excluded_ips(tmp_path: pathlib.Path) -> None:
    """With no excluded address at all, nothing is removed."""
    _write_cache(cache_directory=tmp_path)

    records = remove_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[], use_encryption=False, workers=1)

    assert records == {}
    assert (tmp_path / "extraction" / _DESCRIPTION_KEY / "ips.txt").exists()


@pytest.mark.ai_generated
def test_remove_excluded_ip_records_requires_an_extraction_directory(tmp_path: pathlib.Path) -> None:
    """A cache without an extraction directory is refused."""
    with pytest.raises(FileNotFoundError, match="No extraction directory"):
        remove_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("start_record", "end_record"),
    [
        # A log whose extraction started but has not ended, whether or not the end record exists yet
        ("log-1\nlog-2\n", "log-1\n"),
        ("log-1\n", None),
    ],
)
def test_remove_excluded_ip_records_refuses_an_unfinished_extraction(
    tmp_path: pathlib.Path, start_record: str, end_record: str | None
) -> None:
    """A running or interrupted extraction leaves the cache untouched."""
    _write_cache(cache_directory=tmp_path)
    records_directory = tmp_path / "records"
    records_directory.mkdir()
    (records_directory / "S3LogAccessExtractor_file-processing-start.txt").write_text(start_record)
    if end_record is not None:
        (records_directory / "S3LogAccessExtractor_file-processing-end.txt").write_text(end_record)
    data_directory = tmp_path / "extraction" / _DATA_KEY
    original_lines = _read_object_key(object_key_directory=data_directory)

    with pytest.raises(RuntimeError, match="started but has not finished"):
        remove_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1)

    assert _read_object_key(object_key_directory=data_directory) == original_lines


@pytest.mark.ai_generated
def test_remove_excluded_ip_records_refuses_a_misaligned_object_key(tmp_path: pathlib.Path) -> None:
    """An object key whose files are not line-aligned is refused and left as it was."""
    _write_cache(cache_directory=tmp_path)
    data_directory = tmp_path / "extraction" / _DATA_KEY
    (data_directory / "bytes_sent.txt").write_text("1\n")
    original_lines = _read_object_key(object_key_directory=data_directory)

    with pytest.raises(RuntimeError, match="not line-aligned"):
        remove_excluded_ip_records(cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1)

    assert _read_object_key(object_key_directory=data_directory) == original_lines


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("extra_arguments", "expected_output", "expected_remaining_records"),
    [
        (
            [],
            "Would remove 5 records of 2 excluded IP addresses across 2 object keys",
            {_DATA_KEY: {_ACTOR_IP: 2}, _DESCRIPTION_KEY: {_ACTOR_IP: 1}},
        ),
        (["--confirm"], "Removed 5 records of 2 excluded IP addresses across 2 object keys", {}),
    ],
)
def test_cli_reset_excluded(
    monkeypatch: pytest.MonkeyPatch,
    dotenv_file_path: pathlib.Path,
    tmp_path: pathlib.Path,
    extra_arguments: list[str],
    expected_output: str,
    expected_remaining_records: dict[str, dict[str, int]],
) -> None:
    """The records are only listed without '--confirm' and removed with it, without printing the addresses."""
    monkeypatch.setenv(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, _ACTOR_IP)
    _write_object_key(
        object_key_directory=tmp_path / "extraction" / _DATA_KEY, ips=[_OTHER_IP, _ACTOR_IP, _ACTOR_IP, _ANOTHER_IP]
    )
    _write_object_key(object_key_directory=tmp_path / "extraction" / _DESCRIPTION_KEY, ips=[_ACTOR_IP, _ANOTHER_IP])

    result = click.testing.CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "reset",
            "excluded",
            "--cache",
            str(tmp_path),
            "--encryption",
            "false",
            "--workers",
            "1",
            "--exclude",
            _ANOTHER_IP,
            *extra_arguments,
        ],
    )

    assert result.exit_code == 0, result.output
    assert f"{_DATA_KEY}: 3 records" in result.output
    assert f"{_DESCRIPTION_KEY}: 2 records" in result.output
    assert expected_output in result.output
    for ip in (_ACTOR_IP, _ANOTHER_IP):
        assert ip not in result.output

    remaining_records = find_excluded_ip_records(
        cache_directory=tmp_path, excluded_ips=[_ACTOR_IP], use_encryption=False, workers=1
    )
    assert remaining_records == expected_remaining_records


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("dotenv_content", "extra_arguments", "expected_output"),
    [
        (None, ["--confirm"], "No excluded IP addresses are configured"),
        (f"{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}=198.51.100.1\n", [], "No records of the 1 excluded IP addresses"),
        (
            f"{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}=198.51.100.1\n",
            ["--confirm"],
            "No records of the 1 excluded IP addresses",
        ),
    ],
)
def test_cli_reset_excluded_without_records(
    dotenv_file_path: pathlib.Path,
    tmp_path: pathlib.Path,
    dotenv_content: str | None,
    extra_arguments: list[str],
    expected_output: str,
) -> None:
    """With no excluded address, or none with a record in the cache, the command passes and changes nothing."""
    if dotenv_content is not None:
        dotenv_file_path.write_text(dotenv_content)
    _write_cache(cache_directory=tmp_path)

    result = click.testing.CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        ["reset", "excluded", "--cache", str(tmp_path), "--encryption", "false", "--workers", "1", *extra_arguments],
    )

    assert result.exit_code == 0, result.output
    assert expected_output in result.output
    assert (tmp_path / "extraction" / _DESCRIPTION_KEY / "ips.txt").exists()

"""Tests for checking whether an extraction has anything to do, through `extract --check` and its methods."""

import pathlib
import shutil
import typing

import pytest
from click.testing import CliRunner
from conftest import build_key_inventory_directory

import s3_log_extraction
from s3_log_extraction.extractors import RemoteS3LogAccessExtractor, S3LogAccessExtractor

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_SOURCE_BUCKET = "my-bucket"
_KEYS = (
    "2024/01/01/2024-01-01-00-00-00-AAAA",
    "2024/01/01/2024-01-01-00-05-00-BBBB",
    "2024/01/02/2024-01-02-00-00-00-CCCC",
)
_PENDING_EXIT_CODE = 3
_PENDING_OUTPUT = "Log files are pending extraction.\n"
_NOT_PENDING_OUTPUT = "No log files are pending extraction.\n"


@pytest.fixture
def base_temporary_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Create the temporary directories of both extractors under a known base, so that any directory shows."""
    base_temporary_directory = tmp_path / "tmp"
    base_temporary_directory.mkdir()
    for module in ("_remote_s3_log_access_extractor", "_s3_log_access_extractor"):
        monkeypatch.setattr(
            f"s3_log_extraction.extractors.{module}.get_base_temporary_directory", lambda: base_temporary_directory
        )
    return base_temporary_directory


@pytest.fixture
def inventory_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """An inventory listing every key of `_KEYS`."""
    inventory_directory = build_key_inventory_directory(tmp_path, source_bucket=_SOURCE_BUCKET, keys=list(_KEYS))
    return inventory_directory


def _write_records(*, records_directory: pathlib.Path, prefix: str, started: list[str], ended: list[str]) -> None:
    records_directory.mkdir(parents=True, exist_ok=True)
    (records_directory / f"{prefix}-start.txt").write_text("".join(f"{name}\n" for name in started))
    (records_directory / f"{prefix}-end.txt").write_text("".join(f"{name}\n" for name in ended))


def _write_remote_records(*, cache_directory: pathlib.Path, number_of_processed_keys: int) -> None:
    processed_names = [key.split("/")[-1] for key in _KEYS[:number_of_processed_keys]]
    _write_records(
        records_directory=cache_directory / "records",
        prefix="RemoteS3LogAccessExtractor_s3-url-processing",
        started=processed_names,
        ended=processed_names,
    )


def _read_directory(directory: pathlib.Path) -> dict[str, str]:
    contents = {str(path.relative_to(directory)): path.read_text() for path in directory.rglob("*") if path.is_file()}
    return contents


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_processed_keys", "expected_unprocessed_keys"),
    [(0, _KEYS), (1, _KEYS[1:]), (3, ())],
)
def test_remote_extractor_reports_unprocessed_s3_urls(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    inventory_directory: pathlib.Path,
    number_of_processed_keys: int,
    expected_unprocessed_keys: tuple[str, ...],
) -> None:
    """Both the full list and the yes-or-no check leave out the keys in the end record."""
    cache_directory = tmp_path / "cache"
    _write_remote_records(cache_directory=cache_directory, number_of_processed_keys=number_of_processed_keys)
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory)

    unprocessed_s3_urls = extractor.get_unprocessed_s3_urls(
        s3_root=f"s3://{_SOURCE_BUCKET}", inventory_directory=inventory_directory
    )
    has_unprocessed_s3_urls = extractor.has_unprocessed_s3_urls(
        s3_root=f"s3://{_SOURCE_BUCKET}", inventory_directory=inventory_directory
    )

    assert unprocessed_s3_urls == [f"s3://{_SOURCE_BUCKET}/{key}" for key in expected_unprocessed_keys]
    assert has_unprocessed_s3_urls is (len(expected_unprocessed_keys) > 0)
    assert list(base_temporary_directory.iterdir()) == []


@pytest.mark.ai_generated
def test_has_unprocessed_s3_urls_stops_at_the_first_unprocessed_log(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, base_temporary_directory: pathlib.Path
) -> None:
    """The inventory is not read past the first log file that is not in the end record."""
    cache_directory = tmp_path / "cache"
    _write_remote_records(cache_directory=cache_directory, number_of_processed_keys=1)
    read_urls: list[str] = []

    def _recording_inventory(*, inventory_directory: pathlib.Path, s3_root: str) -> typing.Iterator[tuple[str, str]]:
        for key in _KEYS:
            s3_url = f"{s3_root}/{key}"
            read_urls.append(s3_url)
            yield key[:10].replace("/", "-"), s3_url

    monkeypatch.setattr(
        "s3_log_extraction.extractors._remote_s3_log_access_extractor._iter_dated_s3_urls_from_local_inventory",
        _recording_inventory,
    )
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory)

    has_unprocessed_s3_urls = extractor.has_unprocessed_s3_urls(
        s3_root=f"s3://{_SOURCE_BUCKET}", inventory_directory=tmp_path
    )

    assert has_unprocessed_s3_urls is True
    assert read_urls == [f"s3://{_SOURCE_BUCKET}/{key}" for key in _KEYS[:2]]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_processed_keys", "expected_output", "expected_exit_code"),
    [
        (0, _PENDING_OUTPUT, _PENDING_EXIT_CODE),
        (2, _PENDING_OUTPUT, _PENDING_EXIT_CODE),
        (3, _NOT_PENDING_OUTPUT, 0),
    ],
)
def test_cli_extract_check_in_remote_mode(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    inventory_directory: pathlib.Path,
    number_of_processed_keys: int,
    expected_output: str,
    expected_exit_code: int,
) -> None:
    """
    The check reports whether anything is pending through its exit status, and leaves neither a temporary
    directory nor any change to the cache behind.
    """
    cache_directory = tmp_path / "cache"
    _write_remote_records(cache_directory=cache_directory, number_of_processed_keys=number_of_processed_keys)
    cache_before = _read_directory(cache_directory)

    result = CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            f"s3://{_SOURCE_BUCKET}",
            "--mode",
            "remote",
            "--inventory",
            str(inventory_directory),
            "--cache",
            str(cache_directory),
            "--check",
        ],
    )

    assert result.output == expected_output
    assert result.exit_code == expected_exit_code
    assert list(base_temporary_directory.iterdir()) == []
    assert _read_directory(cache_directory) == cache_before


@pytest.mark.ai_generated
def test_cli_extract_check_raises_on_corrupted_records(
    tmp_path: pathlib.Path, base_temporary_directory: pathlib.Path, inventory_directory: pathlib.Path
) -> None:
    """A file started but never finished by a previous run is an error, not pending work."""
    cache_directory = tmp_path / "cache"
    _write_records(
        records_directory=cache_directory / "records",
        prefix="RemoteS3LogAccessExtractor_s3-url-processing",
        started=[_KEYS[0].split("/")[-1]],
        ended=[],
    )

    result = CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            f"s3://{_SOURCE_BUCKET}",
            "--mode",
            "remote",
            "--inventory",
            str(inventory_directory),
            "--cache",
            str(cache_directory),
            "--check",
        ],
    )

    assert isinstance(result.exception, ValueError)
    assert result.exit_code == 1
    assert list(base_temporary_directory.iterdir()) == []


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_extracted_files", "expected_output", "expected_exit_code"),
    [(0, _PENDING_OUTPUT, _PENDING_EXIT_CODE), (2, _PENDING_OUTPUT, _PENDING_EXIT_CODE), (3, _NOT_PENDING_OUTPUT, 0)],
)
def test_cli_extract_check_in_local_mode(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    number_of_extracted_files: int,
    expected_output: str,
    expected_exit_code: int,
) -> None:
    """Local mode checks the log files of the directory against the end record in the same way."""
    log_directory = tmp_path / "logs"
    shutil.copytree(src=_EXAMPLE_LOGS_DIRECTORY, dst=log_directory)
    log_names = sorted(path.name for path in log_directory.iterdir())
    cache_directory = tmp_path / "cache"
    _write_records(
        records_directory=cache_directory / "records",
        prefix="S3LogAccessExtractor_file-processing",
        started=log_names[:number_of_extracted_files],
        ended=log_names[:number_of_extracted_files],
    )
    cache_before = _read_directory(cache_directory)

    result = CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        ["extract", str(log_directory), "--cache", str(cache_directory), "--check"],
    )

    assert len(log_names) == 3
    assert result.output == expected_output
    assert result.exit_code == expected_exit_code
    assert list(base_temporary_directory.iterdir()) == []
    assert _read_directory(cache_directory) == cache_before


@pytest.mark.ai_generated
def test_has_unextracted_files_matches_a_full_extraction(tmp_path: pathlib.Path) -> None:
    """A directory reads as pending before it is extracted and as done after."""
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()

    has_unextracted_files_before = S3LogAccessExtractor(
        cache_directory=cache_directory, use_encryption=False
    ).has_unextracted_files(directory=_EXAMPLE_LOGS_DIRECTORY)
    S3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False).extract_directory(
        directory=_EXAMPLE_LOGS_DIRECTORY, workers=1
    )
    has_unextracted_files_after = S3LogAccessExtractor(
        cache_directory=cache_directory, use_encryption=False
    ).has_unextracted_files(directory=_EXAMPLE_LOGS_DIRECTORY)

    assert has_unextracted_files_before is True
    assert has_unextracted_files_after is False

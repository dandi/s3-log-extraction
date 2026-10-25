"""Tests for checking the pending work of a remote extraction without extracting it."""

import pathlib

import pytest
from click.testing import CliRunner
from conftest import build_key_inventory_directory

import s3_log_extraction
from s3_log_extraction.extractors import RemoteS3LogAccessExtractor

_SOURCE_BUCKET = "my-bucket"
_KEYS = (
    "2024/01/01/2024-01-01-00-00-00-AAAA",
    "2024/01/01/2024-01-01-00-05-00-BBBB",
    "2024/01/02/2024-01-02-00-00-00-CCCC",
)


@pytest.fixture
def base_temporary_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Create the temporary directories of the extractor under a known base so that leaks can be seen."""
    base_temporary_directory = tmp_path / "tmp"
    base_temporary_directory.mkdir()
    monkeypatch.setattr(
        "s3_log_extraction.extractors._remote_s3_log_access_extractor.get_base_temporary_directory",
        lambda: base_temporary_directory,
    )
    return base_temporary_directory


def _write_records(*, cache_directory: pathlib.Path, started: list[str], ended: list[str]) -> None:
    records_directory = cache_directory / "records"
    records_directory.mkdir(parents=True, exist_ok=True)
    (records_directory / "RemoteS3LogAccessExtractor_s3-url-processing-start.txt").write_text(
        "".join(f"{name}\n" for name in started)
    )
    (records_directory / "RemoteS3LogAccessExtractor_s3-url-processing-end.txt").write_text(
        "".join(f"{name}\n" for name in ended)
    )


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_processed_keys", "expected_unprocessed_keys"),
    [(0, _KEYS), (1, _KEYS[1:]), (3, ())],
)
def test_get_unprocessed_s3_urls_excludes_processed_keys(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    number_of_processed_keys: int,
    expected_unprocessed_keys: tuple[str, ...],
) -> None:
    """Only the inventory keys absent from the end record are returned."""
    cache_directory = tmp_path / "cache"
    processed_names = [key.split("/")[-1] for key in _KEYS[:number_of_processed_keys]]
    _write_records(cache_directory=cache_directory, started=processed_names, ended=processed_names)
    inventory_directory = build_key_inventory_directory(tmp_path, source_bucket=_SOURCE_BUCKET, keys=list(_KEYS))

    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory)
    unprocessed_s3_urls = extractor.get_unprocessed_s3_urls(
        s3_root=f"s3://{_SOURCE_BUCKET}", inventory_directory=inventory_directory
    )

    assert sorted(unprocessed_s3_urls) == [f"s3://{_SOURCE_BUCKET}/{key}" for key in expected_unprocessed_keys]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("number_of_processed_keys", "flags", "expected_output", "expected_exit_code"),
    [
        (0, [], "3\n", 0),
        (0, ["--exit-code"], "3\n", 1),
        (2, ["--exit-code"], "1\n", 1),
        (3, [], "0\n", 0),
        (3, ["--exit-code"], "0\n", 0),
    ],
)
def test_pending_cli_reports_count_and_cleans_up(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
    number_of_processed_keys: int,
    flags: list[str],
    expected_output: str,
    expected_exit_code: int,
) -> None:
    """
    The command prints the number of pending files, exits nonzero on request when any are pending, and leaves
    neither a temporary directory nor any change to the records behind.
    """
    cache_directory = tmp_path / "cache"
    processed_names = [key.split("/")[-1] for key in _KEYS[:number_of_processed_keys]]
    _write_records(cache_directory=cache_directory, started=processed_names, ended=processed_names)
    records_before = {path.name: path.read_text() for path in (cache_directory / "records").iterdir()}
    inventory_directory = build_key_inventory_directory(tmp_path, source_bucket=_SOURCE_BUCKET, keys=list(_KEYS))

    result = CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "pending",
            f"s3://{_SOURCE_BUCKET}",
            "--inventory",
            str(inventory_directory),
            "--cache",
            str(cache_directory),
            *flags,
        ],
    )

    records_after = {path.name: path.read_text() for path in (cache_directory / "records").iterdir()}
    assert result.output == expected_output
    assert result.exit_code == expected_exit_code
    assert list(base_temporary_directory.iterdir()) == []
    assert records_after == records_before


@pytest.mark.ai_generated
def test_pending_cli_raises_on_corrupted_records(
    tmp_path: pathlib.Path,
    base_temporary_directory: pathlib.Path,
) -> None:
    """A file started but never finished by a previous run is reported as corruption rather than counted."""
    cache_directory = tmp_path / "cache"
    _write_records(cache_directory=cache_directory, started=[_KEYS[0].split("/")[-1]], ended=[])
    inventory_directory = build_key_inventory_directory(tmp_path, source_bucket=_SOURCE_BUCKET, keys=list(_KEYS))

    result = CliRunner().invoke(
        s3_log_extraction.s3logextraction_cli,
        ["pending", f"s3://{_SOURCE_BUCKET}", "--inventory", str(inventory_directory), "--cache", str(cache_directory)],
    )

    assert isinstance(result.exception, ValueError)
    assert result.exit_code != 0
    assert list(base_temporary_directory.iterdir()) == []

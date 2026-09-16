"""Tests for the base directory that extraction runs create their temporary directories inside."""

import pathlib
import secrets
import shutil

import pytest

import s3_log_extraction
from s3_log_extraction.config import set_base_temporary_directory, unset_base_temporary_directory
from s3_log_extraction.extractors import RemoteS3LogAccessExtractor, S3LogAccessExtractor

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_EXTRACTOR_CLASSES = (S3LogAccessExtractor, RemoteS3LogAccessExtractor)
_STRONG_PASSWORD = secrets.token_urlsafe(32)


@pytest.fixture(autouse=True)
def strong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """The encryption key derivation rejects weak passwords, so use a randomly generated secret."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)


@pytest.fixture
def cache_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """An empty cache directory for an extractor to lay out its extraction and records subdirectories in."""
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()
    return cache_directory


@pytest.fixture
def isolated_config(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the configuration file at a temporary location rather than the user's real one."""
    config_file_path = tmp_path / "config.yaml"
    monkeypatch.setattr("s3_log_extraction.config._config.S3_LOG_EXTRACTION_CONFIG_FILE_PATH", config_file_path)
    return config_file_path


@pytest.mark.ai_generated
@pytest.mark.parametrize("extractor_class", _EXTRACTOR_CLASSES)
def test_extractor_defaults_to_system_temporary_directory(
    isolated_config: pathlib.Path, cache_directory: pathlib.Path, extractor_class: type
) -> None:
    """With nothing configured, a run works under the system temporary directory."""
    extractor = extractor_class(cache_directory=cache_directory, use_encryption=False)

    assert extractor.base_temporary_directory is None
    assert extractor.temporary_directory.is_dir() is True
    assert extractor.temporary_directory.name.startswith("s3logextraction-") is True


@pytest.mark.ai_generated
@pytest.mark.parametrize("extractor_class", _EXTRACTOR_CLASSES)
def test_extractor_uses_configured_base_temporary_directory(
    isolated_config: pathlib.Path, cache_directory: pathlib.Path, tmp_path: pathlib.Path, extractor_class: type
) -> None:
    """A configured base temporary directory is where a run creates its own temporary directory."""
    base_temporary_directory = tmp_path / "tmp"
    set_base_temporary_directory(base_temporary_directory)

    extractor = extractor_class(cache_directory=cache_directory, use_encryption=False)

    assert extractor.base_temporary_directory == base_temporary_directory
    assert extractor.temporary_directory.parent == base_temporary_directory


@pytest.mark.ai_generated
@pytest.mark.parametrize("extractor_class", _EXTRACTOR_CLASSES)
def test_extractor_argument_overrides_configured_base_temporary_directory(
    isolated_config: pathlib.Path, cache_directory: pathlib.Path, tmp_path: pathlib.Path, extractor_class: type
) -> None:
    """The directory passed to an extractor wins over the configured one."""
    set_base_temporary_directory(tmp_path / "configured_tmp")
    requested_base_temporary_directory = tmp_path / "requested_tmp"

    extractor = extractor_class(
        cache_directory=cache_directory,
        use_encryption=False,
        base_temporary_directory=requested_base_temporary_directory,
    )

    assert extractor.temporary_directory.parent == requested_base_temporary_directory


@pytest.mark.ai_generated
def test_base_temporary_directory_is_created_when_missing(
    isolated_config: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A base temporary directory that does not exist yet is created, including its parents."""
    base_temporary_directory = tmp_path / "nested" / "tmp"

    set_base_temporary_directory(base_temporary_directory)

    assert base_temporary_directory.is_dir() is True


@pytest.mark.ai_generated
def test_unset_base_temporary_directory_clears_the_configuration(
    isolated_config: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Unsetting the only configured value clears it from the file rather than leaving it in place."""
    set_base_temporary_directory(tmp_path / "tmp")

    unset_base_temporary_directory()

    assert s3_log_extraction.config.get_config() == {}
    assert s3_log_extraction.config.get_base_temporary_directory() is None


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
def test_extraction_leaves_no_temporary_directories_behind(
    isolated_config: pathlib.Path, cache_directory: pathlib.Path, tmp_path: pathlib.Path, workers: int
) -> None:
    """Serial and parallel runs alike clean up every temporary directory they created under the base."""
    base_temporary_directory = tmp_path / "tmp"
    extractor = S3LogAccessExtractor(
        cache_directory=cache_directory,
        use_encryption=True,
        base_temporary_directory=base_temporary_directory,
    )

    extractor.extract_directory(directory=_EXAMPLE_LOGS_DIRECTORY, workers=workers)

    assert list(base_temporary_directory.iterdir()) == []


@pytest.mark.ai_generated
def test_serial_extraction_after_a_parallel_run_still_writes_into_the_cache(
    isolated_config: pathlib.Path, cache_directory: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A parallel run must not leave later serial extraction pointed at a temporary directory."""
    log_file_paths = sorted(_EXAMPLE_LOGS_DIRECTORY.iterdir())
    parallel_directory = tmp_path / "parallel_logs"
    parallel_directory.mkdir()
    shutil.copy(src=log_file_paths[0], dst=parallel_directory / log_file_paths[0].name)

    base_temporary_directory = tmp_path / "tmp"
    extractor = S3LogAccessExtractor(
        cache_directory=cache_directory,
        use_encryption=False,
        base_temporary_directory=base_temporary_directory,
    )
    extractor.extract_directory(directory=parallel_directory, workers=2)
    extraction_directory = cache_directory / "extraction"
    lines_after_parallel_run = sum(
        len(file_path.read_text().splitlines()) for file_path in extraction_directory.rglob(pattern="*.txt")
    )

    extractor.extract_file(file_path=log_file_paths[1])

    lines_after_serial_run = sum(
        len(file_path.read_text().splitlines()) for file_path in extraction_directory.rglob(pattern="*.txt")
    )

    assert lines_after_serial_run > lines_after_parallel_run
    assert list(base_temporary_directory.iterdir()) == []

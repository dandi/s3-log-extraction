"""
Tests for the remote extractor, driven over the bundled example logs served as ``file://`` URLs.

The extractor downloads each log through ``fsspec``, which serves local files under the ``file`` protocol exactly as
it serves S3 objects under ``s3``. Listing the bucket is the only step that needs S3, so that is what is stood in for.
"""

import os
import pathlib
import secrets
import shutil
import unittest.mock

import pytest
from conftest import read_extracted_ips

import s3_log_extraction
from s3_log_extraction.extractors import RemoteS3LogAccessExtractor

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_EXPECTED_OUTPUT_DIRECTORY = pathlib.Path(__file__).parent / "expected_output"
_EXAMPLE_LOG_URLS = sorted(log_file_path.absolute().as_uri() for log_file_path in _EXAMPLE_LOGS_DIRECTORY.iterdir())
_S3_ROOT = "s3://my-logs-bucket"
_STRONG_PASSWORD = secrets.token_urlsafe(32)


@pytest.fixture(autouse=True)
def placeholder_aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """The extractor insists on AWS credentials before it starts, though nothing here ever reaches S3."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "placeholder")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "placeholder")


@pytest.fixture(autouse=True)
def strong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """The encryption key derivation rejects weak passwords, so use a randomly generated secret."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)


@pytest.fixture
def inventory_of_example_logs(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """
    An inventory directory whose reading lists the example logs as ``file://`` URLs.

    The inventory walk itself is covered elsewhere; here it is stood in for so that the URLs it yields point at
    local files rather than at S3 objects.
    """
    inventory_directory = tmp_path / "inventory"
    inventory_directory.mkdir()
    monkeypatch.setattr(
        "s3_log_extraction.extractors._remote_s3_log_access_extractor._read_s3_urls_from_local_inventory",
        lambda *, inventory_directory, s3_root: {"2020-01-01": list(_EXAMPLE_LOG_URLS)},
    )
    return inventory_directory


@pytest.fixture
def cache_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """An empty cache directory for an extractor to lay out its extraction and records subdirectories in."""
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()
    return cache_directory


def _read_record(record_file_path: pathlib.Path) -> list[str]:
    return record_file_path.read_text().splitlines() if record_file_path.exists() else []


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
def test_extract_s3_bucket_matches_expected_output(
    cache_directory: pathlib.Path, inventory_of_example_logs: pathlib.Path, workers: int
) -> None:
    """Serial and batched parallel runs alike produce the same extraction as the local extractor does."""
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)

    extractor.extract_s3_bucket(
        s3_root=_S3_ROOT, workers=workers, batch_size=2, inventory_directory=inventory_of_example_logs
    )

    s3_log_extraction.testing.assert_filetree_matches(
        test_dir=cache_directory / "extraction", expected_dir=_EXPECTED_OUTPUT_DIRECTORY / "extraction"
    )
    expected_record_keys = sorted(log_file_path.name for log_file_path in _EXAMPLE_LOGS_DIRECTORY.iterdir())
    assert sorted(_read_record(extractor.s3_url_processing_start_record_file_path)) == expected_record_keys
    assert sorted(_read_record(extractor.s3_url_processing_end_record_file_path)) == expected_record_keys
    assert extractor.temporary_directory.exists() is False


@pytest.mark.ai_generated
@pytest.mark.parametrize("limit", [1, 2])
def test_extract_s3_bucket_respects_limit(
    cache_directory: pathlib.Path, inventory_of_example_logs: pathlib.Path, limit: int
) -> None:
    """Only up to `limit` logs should be extracted per call."""
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)

    extractor.extract_s3_bucket(s3_root=_S3_ROOT, limit=limit, workers=1, inventory_directory=inventory_of_example_logs)

    assert len(_read_record(extractor.s3_url_processing_end_record_file_path)) == limit


@pytest.mark.ai_generated
def test_extract_s3_bucket_resumes_from_the_record(
    cache_directory: pathlib.Path, inventory_of_example_logs: pathlib.Path
) -> None:
    """A later run picks up the logs a previous run did not get to, and never repeats one it finished."""
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)
    extractor.extract_s3_bucket(s3_root=_S3_ROOT, limit=1, workers=1, inventory_directory=inventory_of_example_logs)
    first_run_record = _read_record(extractor.s3_url_processing_end_record_file_path)

    resumed_extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)
    resumed_extractor.extract_s3_bucket(s3_root=_S3_ROOT, workers=1, inventory_directory=inventory_of_example_logs)

    end_record = _read_record(resumed_extractor.s3_url_processing_end_record_file_path)
    assert len(first_run_record) == 1
    assert sorted(end_record) == sorted(log_file_path.name for log_file_path in _EXAMPLE_LOGS_DIRECTORY.iterdir())
    assert len(end_record) == len(set(end_record))  # No duplicated entries
    s3_log_extraction.testing.assert_filetree_matches(
        test_dir=cache_directory / "extraction", expected_dir=_EXPECTED_OUTPUT_DIRECTORY / "extraction"
    )


@pytest.mark.ai_generated
def test_extract_s3_bucket_detects_record_corruption(
    cache_directory: pathlib.Path, inventory_of_example_logs: pathlib.Path
) -> None:
    """A log that started but never finished means the extraction cache is unreliable and must be reset."""
    records_directory = cache_directory / "records"
    records_directory.mkdir()
    (records_directory / "RemoteS3LogAccessExtractor_s3-url-processing-start.txt").write_text("log_a\nlog_b\n")
    (records_directory / "RemoteS3LogAccessExtractor_s3-url-processing-end.txt").write_text("log_a\n")
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)

    with pytest.raises(ValueError, match="Record corruption from previous run detected"):
        extractor.extract_s3_bucket(s3_root=_S3_ROOT, workers=1, inventory_directory=inventory_of_example_logs)


@pytest.mark.ai_generated
@pytest.mark.parametrize("workers", [1, 2])
def test_extract_s3_bucket_stops_when_signalled(
    cache_directory: pathlib.Path, inventory_of_example_logs: pathlib.Path, workers: int
) -> None:
    """
    With the stop file already present, no log is extracted and the run cleans up after itself.

    The serial path checks the file before every log, the batched path before every batch is submitted.
    """
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)
    extractor.stop_file_path.touch()

    extractor.extract_s3_bucket(
        s3_root=_S3_ROOT, workers=workers, batch_size=1, inventory_directory=inventory_of_example_logs
    )

    assert _read_record(extractor.s3_url_processing_end_record_file_path) == []
    assert list(extractor.extraction_directory.rglob(pattern="*.txt")) == []
    assert extractor.temporary_directory.exists() is False


@pytest.mark.ai_generated
def test_extract_s3_bucket_with_encryption_matches_plaintext_extraction(
    tmp_path: pathlib.Path, inventory_of_example_logs: pathlib.Path
) -> None:
    """Encrypting the extracted IPs should not change which IPs are recorded, nor leave any readable on disk."""
    plaintext_cache_directory = tmp_path / "plaintext"
    plaintext_cache_directory.mkdir()
    encrypted_cache_directory = tmp_path / "encrypted"
    encrypted_cache_directory.mkdir()

    plaintext_extractor = RemoteS3LogAccessExtractor(cache_directory=plaintext_cache_directory, use_encryption=False)
    plaintext_extractor.extract_s3_bucket(s3_root=_S3_ROOT, workers=1, inventory_directory=inventory_of_example_logs)
    encrypted_extractor = RemoteS3LogAccessExtractor(cache_directory=encrypted_cache_directory, use_encryption=True)
    encrypted_extractor.extract_s3_bucket(s3_root=_S3_ROOT, workers=1, inventory_directory=inventory_of_example_logs)

    plaintext_ips = read_extracted_ips(plaintext_extractor.extraction_directory, use_encryption=False)
    decrypted_ips = read_extracted_ips(encrypted_extractor.extraction_directory, use_encryption=True)
    raw_contents = b"".join(
        file_path.read_bytes() for file_path in encrypted_extractor.extraction_directory.rglob(pattern="ips.txt")
    )

    assert len(plaintext_ips) > 0
    assert sorted(decrypted_ips) == sorted(plaintext_ips)
    for ip_address in set(decrypted_ips):
        assert ip_address.encode() not in raw_contents


@pytest.mark.ai_generated
def test_extract_s3_url_in_parallel_mode_writes_under_its_process_directory(cache_directory: pathlib.Path) -> None:
    """A worker writes beneath its own process-ID directory, to be merged into the cache once the batch is done."""
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)

    extractor._extract_s3_url(s3_url=_EXAMPLE_LOG_URLS[0], enable_stop=False, parallel_mode=True)

    worker_directory = extractor.temporary_directory / str(os.getpid())
    assert len(list(worker_directory.rglob(pattern="*.txt"))) > 0
    assert list(extractor.extraction_directory.rglob(pattern="*.txt")) == []
    assert _read_record(extractor.s3_url_processing_end_record_file_path) == [_EXAMPLE_LOG_URLS[0].split("/")[-1]]

    shutil.rmtree(path=extractor.temporary_directory, ignore_errors=True)


def _fake_s5cmd_listing(command: str, error_message: str) -> str:
    """Answer the ``s5cmd ls`` calls of a bucket laid out as ``<year>/<month>/<day>/<log>``."""
    listed_prefix = command.removeprefix("s5cmd ls ").removesuffix("/")
    depth = listed_prefix.count("/") - _S3_ROOT.count("/")
    match depth:
        case 0:
            return "                                  DIR  2024/\n"
        case 1:
            return "                                  DIR  01/\n                                  DIR  02/\n"
        case 2:
            month = listed_prefix.split("/")[-1]
            days = ("01", "02") if month == "01" else ("01", "02", "03")
            return "".join(f"                                  DIR  {day}/\n" for day in days)
        case _:
            year, month, day = listed_prefix.split("/")[-3:]
            return "".join(
                f"2024/01/01 00:00:00               1234 {year}-{month}-{day}-00-0{index}-00-{index * 16}\n"
                for index in ("A", "B")
            )


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_remote_walks_the_bucket_and_buffers_the_latest_days(
    cache_directory: pathlib.Path,
) -> None:
    """
    The bucket is walked year by month by day, the two most recent days are left for AWS to finish delivering,
    and logs already recorded as processed are left out.
    """
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory, use_encryption=False)
    extractor.s3_url_processing_end_record = {"2024-01-02-00-0A-00-" + "A" * 16}

    with (
        unittest.mock.patch(
            "s3_log_extraction.extractors._remote_s3_log_access_extractor._deploy_subprocess",
            side_effect=_fake_s5cmd_listing,
        ),
        pytest.warns(UserWarning, match="Consider setting up AWS S3 Inventory"),
    ):
        unprocessed_s3_urls = extractor._get_unprocessed_s3_urls_from_remote(s3_root=_S3_ROOT)

    # Of the five days listed, 2024-02-02 and 2024-02-03 are held back, and one log of 2024-01-02 is already done
    assert unprocessed_s3_urls == [
        f"{_S3_ROOT}/2024/01/01/2024-01-01-00-0A-00-{'A' * 16}",
        f"{_S3_ROOT}/2024/01/01/2024-01-01-00-0B-00-{'B' * 16}",
        f"{_S3_ROOT}/2024/01/02/2024-01-02-00-0B-00-{'B' * 16}",
        f"{_S3_ROOT}/2024/02/01/2024-02-01-00-0A-00-{'A' * 16}",
        f"{_S3_ROOT}/2024/02/01/2024-02-01-00-0B-00-{'B' * 16}",
    ]

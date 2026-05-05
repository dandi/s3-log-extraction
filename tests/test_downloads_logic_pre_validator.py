"""Tests for the DownloadsLogicPreValidator."""

import pathlib

import pytest

from s3_log_extraction.validate import DownloadsLogicPreValidator

# A minimal but complete S3 log line prefix/suffix template:
#   <hash> <bucket> [<datetime>] <ip> <iam> <req_id> <operation> <key>
#   "<METHOD> /<key> HTTP/1.x" <status> <error> <bytes_sent> <total_bytes> ...
_LOG_PREFIX = "abc123 bucket [01/Jan/2020:00:00:00 +0000] 10.0.0.1 - REQ123 REST.GET.OBJECT test/file.dat "
_LOG_SUFFIX = ' 10 5 "-" "TestAgent" - - - - - - - - -'


def _make_log_line(status: str, bytes_sent: str, total_bytes: str) -> str:
    """
    Build a synthetic S3 log line with the specified status, bytes_sent, and total_bytes fields.

    Parameters
    ----------
    status : str
        The HTTP status code string, e.g. ``"200"`` or ``"206"``.
    bytes_sent : str
        The bytes-sent field value, e.g. ``"1000"`` or ``"-"``.
    total_bytes : str
        The total-bytes (object size) field value, e.g. ``"1000"``.

    Returns
    -------
    str
        A single S3 log line string.
    """
    return f'{_LOG_PREFIX}"GET /test/file.dat HTTP/1.1" {status} - {bytes_sent} {total_bytes}{_LOG_SUFFIX}\n'


@pytest.mark.ai_generated
def test_downloads_logic_valid_complete_download(tmp_path: pathlib.Path) -> None:
    """Validator should pass when bytes_sent equals total_bytes with a 200 status."""
    log_file = tmp_path / "valid.log"
    log_file.write_text(_make_log_line(status="200", bytes_sent="1000", total_bytes="1000"))

    validator = DownloadsLogicPreValidator()
    # Should not raise
    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_downloads_logic_valid_partial_206(tmp_path: pathlib.Path) -> None:
    """Validator should pass when bytes_sent < total_bytes with a 206 status (partial content is expected)."""
    log_file = tmp_path / "partial.log"
    log_file.write_text(_make_log_line(status="206", bytes_sent="100", total_bytes="1000"))

    validator = DownloadsLogicPreValidator()
    # Should not raise
    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_downloads_logic_valid_bytes_sent_dash(tmp_path: pathlib.Path) -> None:
    """Validator should pass when bytes_sent is '-' (not a number), even with a 200 status."""
    log_file = tmp_path / "dash.log"
    log_file.write_text(_make_log_line(status="200", bytes_sent="-", total_bytes="1000"))

    validator = DownloadsLogicPreValidator()
    # Should not raise
    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_downloads_logic_valid_total_bytes_dash(tmp_path: pathlib.Path) -> None:
    """Validator should pass when total_bytes is '-' (not a number), even with a 200 status."""
    log_file = tmp_path / "total_dash.log"
    log_file.write_text(_make_log_line(status="200", bytes_sent="100", total_bytes="-"))

    validator = DownloadsLogicPreValidator()
    # Should not raise
    validator._run_validation(file_path=log_file)


@pytest.mark.ai_generated
def test_downloads_logic_aberrant_bytes_less_than_total(tmp_path: pathlib.Path) -> None:
    """Validator should raise RuntimeError when bytes_sent < total_bytes with a 200 status."""
    log_file = tmp_path / "aberrant.log"
    log_file.write_text(_make_log_line(status="200", bytes_sent="100", total_bytes="1000"))

    validator = DownloadsLogicPreValidator()
    with pytest.raises(RuntimeError, match="Downloads logic pre-check failed"):
        validator._run_validation(file_path=log_file)

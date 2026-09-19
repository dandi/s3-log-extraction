"""Tests for the subprocess and credential helpers the extractors are built on."""

import pathlib

import pytest

from s3_log_extraction.extractors import S3LogAccessExtractor
from s3_log_extraction.extractors._utils import _deploy_subprocess, _handle_aws_credentials

_SINGLE_PROFILE_CREDENTIALS = (
    "[default]\naws_access_key_id = placeholder-id\naws_secret_access_key = placeholder-secret\n"
)
_TWO_PROFILE_CREDENTIALS = _SINGLE_PROFILE_CREDENTIALS + (
    "[other]\naws_access_key_id = other-id\naws_secret_access_key = other-secret\n"
)


@pytest.fixture
def home_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """A home directory of our own, so that the user's real AWS credentials file is never read."""
    home_directory = tmp_path / "home"
    home_directory.mkdir()
    monkeypatch.setenv("HOME", str(home_directory))
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    return home_directory


def _write_credentials_file(home_directory: pathlib.Path, content: str) -> None:
    credentials_file_path = home_directory / ".aws" / "credentials"
    credentials_file_path.parent.mkdir()
    credentials_file_path.write_text(content)


@pytest.mark.ai_generated
def test_handle_aws_credentials_accepts_environment_variables(
    home_directory: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With both variables set, the credentials file is not needed at all."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "placeholder-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "placeholder-secret")

    _handle_aws_credentials()


@pytest.mark.ai_generated
def test_handle_aws_credentials_accepts_a_single_profile_file(home_directory: pathlib.Path) -> None:
    """Without the variables, a credentials file holding exactly one profile is unambiguous."""
    _write_credentials_file(home_directory, content=_SINGLE_PROFILE_CREDENTIALS)

    _handle_aws_credentials()


@pytest.mark.ai_generated
def test_handle_aws_credentials_rejects_an_ambiguous_file(home_directory: pathlib.Path) -> None:
    """A credentials file holding several profiles cannot be chosen from without the variables."""
    _write_credentials_file(home_directory, content=_TWO_PROFILE_CREDENTIALS)

    with pytest.raises(ValueError, match="multiple AWS credentials were found"):
        _handle_aws_credentials()


@pytest.mark.ai_generated
def test_handle_aws_credentials_rejects_a_missing_secret(
    home_directory: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An access key without its secret, and no file to complete it from, is reported as missing."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "placeholder-id")

    with pytest.raises(ValueError, match="Missing environment variables"):
        _handle_aws_credentials()


@pytest.mark.ai_generated
def test_deploy_subprocess_returns_the_output_of_a_successful_command() -> None:
    assert _deploy_subprocess(command="echo hello") == "hello\n"


@pytest.mark.ai_generated
def test_deploy_subprocess_reports_a_failed_command() -> None:
    """A failing command raises with its exit code, the caller's message, and what it wrote."""
    with pytest.raises(RuntimeError, match=r"Error code 3\nlisting failed[\s\S]*stderr: something went wrong"):
        _deploy_subprocess(command="echo 'something went wrong' >&2; exit 3", error_message="listing failed")


@pytest.mark.ai_generated
def test_deploy_subprocess_can_ignore_a_failed_command() -> None:
    """With errors ignored, a failing command yields no output rather than raising."""
    assert _deploy_subprocess(command="exit 3", ignore_errors=True) is None


@pytest.mark.ai_generated
def test_extract_file_reports_a_log_that_cannot_be_read(tmp_path: pathlib.Path) -> None:
    """A log the extraction script cannot open is reported by name rather than passing silently."""
    extractor = S3LogAccessExtractor(cache_directory=tmp_path, use_encryption=False)
    missing_log_file_path = tmp_path / "2020-01-01-05-06-35-0123456789ABCDEF"

    with pytest.raises(RuntimeError, match=f"Extraction failed on {missing_log_file_path}"):
        extractor.extract_file(file_path=missing_log_file_path)

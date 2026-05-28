"""Tests for the ExtractionHeuristicPreValidator."""

import pathlib
import subprocess

import pytest

from s3_log_extraction.validate._extraction_heuristic_pre_validator import (
    EXCLUDED_IP_REGEX_ENCRYPTED,
    ExtractionHeuristicPreValidator,
)


@pytest.mark.ai_generated
def test_excluded_ip_regex_defaults_to_encrypted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", raising=False)
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)

    expected_regex = "^203\\.0\\.113\\.1$"

    def _decrypt_stub(*, encrypted_data: bytes) -> str:
        assert encrypted_data == EXCLUDED_IP_REGEX_ENCRYPTED
        return expected_regex

    monkeypatch.setattr(
        "s3_log_extraction.validate._extraction_heuristic_pre_validator.decrypt_bytes",
        _decrypt_stub,
    )

    validator = ExtractionHeuristicPreValidator()
    assert validator._get_excluded_ip_regex() == expected_regex


@pytest.mark.ai_generated
def test_excluded_ip_regex_plaintext_override(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_regex = "^192\\.0\\.2\\.1$"
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", expected_regex)

    validator = ExtractionHeuristicPreValidator()
    assert validator._get_excluded_ip_regex() == expected_regex


@pytest.mark.ai_generated
def test_excluded_ip_regex_plaintext_requires_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)
    monkeypatch.setenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", "^198\\.51\\.100\\.2$")

    with pytest.raises(EnvironmentError, match="S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX"):
        ExtractionHeuristicPreValidator()


@pytest.mark.ai_generated
def test_run_validation_passes_excluded_ip_regex_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    expected_regex = "^198\\.51\\.100\\.2$"
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", expected_regex)

    captured_env: dict[str, str] = {}

    def _run_stub(
        *, args: str, shell: bool, capture_output: bool, text: bool, env: dict[str, str]
    ) -> subprocess.CompletedProcess:
        assert args.startswith("awk --file ")
        assert shell is True
        assert capture_output is True
        assert text is True
        captured_env.update(env)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("subprocess.run", _run_stub)

    log_path = tmp_path / "test.log"
    log_path.write_text("")

    validator = ExtractionHeuristicPreValidator()
    validator._run_validation(file_path=log_path)
    assert captured_env == {"EXCLUDED_IP_REGEX": expected_regex}

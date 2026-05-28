"""Tests for excluded IP regex configuration in ExtractionHeuristicPreValidator."""

import importlib
import pathlib
import sys
import types

import pytest


def _load_pre_validator_module():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    package_root = repo_root / "src" / "s3_log_extraction"
    validate_root = package_root / "validate"
    utils_root = package_root / "utils"

    if "s3_log_extraction" not in sys.modules:
        top_level_package = types.ModuleType("s3_log_extraction")
        top_level_package.__path__ = [str(package_root)]
        sys.modules["s3_log_extraction"] = top_level_package

    if "s3_log_extraction.validate" not in sys.modules:
        validate_package = types.ModuleType("s3_log_extraction.validate")
        validate_package.__path__ = [str(validate_root)]
        sys.modules["s3_log_extraction.validate"] = validate_package

    if "s3_log_extraction.utils" not in sys.modules:
        utils_package = types.ModuleType("s3_log_extraction.utils")
        utils_package.__path__ = [str(utils_root)]
        sys.modules["s3_log_extraction.utils"] = utils_package

    return importlib.import_module("s3_log_extraction.validate._extraction_heuristic_pre_validator")


@pytest.mark.ai_generated
def test_uses_encrypted_default_regex(monkeypatch: pytest.MonkeyPatch) -> None:
    pre_validator_module = _load_pre_validator_module()

    monkeypatch.delenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", raising=False)
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)
    monkeypatch.delenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", raising=False)
    monkeypatch.setattr(pre_validator_module, "decrypt_bytes", lambda encrypted_data: "decrypted-regex")

    assert pre_validator_module.ExtractionHeuristicPreValidator._get_excluded_ip_regex() == "decrypted-regex"


@pytest.mark.ai_generated
def test_uses_generic_plaintext_regex_override(monkeypatch: pytest.MonkeyPatch) -> None:
    pre_validator_module = _load_pre_validator_module()

    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.setenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", "^10\\.0\\.0\\.1$")
    monkeypatch.delenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", raising=False)

    assert pre_validator_module.ExtractionHeuristicPreValidator._get_excluded_ip_regex() == "^10\\.0\\.0\\.1$"


@pytest.mark.ai_generated
def test_uses_legacy_plaintext_regex_override(monkeypatch: pytest.MonkeyPatch) -> None:
    pre_validator_module = _load_pre_validator_module()

    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)
    monkeypatch.setenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", "^10\\.0\\.0\\.2$")

    assert pre_validator_module.ExtractionHeuristicPreValidator._get_excluded_ip_regex() == "^10\\.0\\.0\\.2$"


@pytest.mark.ai_generated
def test_plaintext_override_requires_regex_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    pre_validator_module = _load_pre_validator_module()

    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.delenv("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX", raising=False)
    monkeypatch.delenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", raising=False)

    with pytest.raises(EnvironmentError, match="S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX"):
        pre_validator_module.ExtractionHeuristicPreValidator._get_excluded_ip_regex()


@pytest.mark.ai_generated
def test_validation_passes_excluded_ip_regex_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    pre_validator_module = _load_pre_validator_module()

    monkeypatch.setattr(
        pre_validator_module.ExtractionHeuristicPreValidator,
        "_get_excluded_ip_regex",
        staticmethod(lambda: "^10\\.0\\.0\\.1$"),
    )

    captured_env = {}

    def _mock_run(*args, **kwargs):
        captured_env.update(kwargs["env"])
        return types.SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr(pre_validator_module.subprocess, "run", _mock_run)

    validator = pre_validator_module.ExtractionHeuristicPreValidator()
    log_file = tmp_path / "test.log"
    log_file.write_text("dummy\n")

    validator._run_validation(file_path=log_file)

    assert captured_env["EXCLUDED_IP_REGEX"] == "^10\\.0\\.0\\.1$"
    assert "DROGON_IP_REGEX" not in captured_env

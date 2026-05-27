import importlib.util
import pathlib
import sys
import types

import pytest


def _load_extraction_heuristic_module():
    source_root = pathlib.Path(__file__).parent.parent / "src" / "s3_log_extraction"

    for name in (
        "s3_log_extraction",
        "s3_log_extraction.validate",
        "s3_log_extraction.utils",
        "s3_log_extraction._regex",
        "s3_log_extraction.utils.encryption",
        "s3_log_extraction.validate._base_validator",
        "s3_log_extraction.validate._extraction_heuristic_pre_validator",
    ):
        sys.modules.pop(name, None)

    package = types.ModuleType("s3_log_extraction")
    package.__path__ = [str(source_root)]  # type: ignore[attr-defined]
    sys.modules["s3_log_extraction"] = package

    validate_package = types.ModuleType("s3_log_extraction.validate")
    validate_package.__path__ = [str(source_root / "validate")]  # type: ignore[attr-defined]
    sys.modules["s3_log_extraction.validate"] = validate_package

    utils_package = types.ModuleType("s3_log_extraction.utils")
    utils_package.__path__ = [str(source_root / "utils")]  # type: ignore[attr-defined]
    sys.modules["s3_log_extraction.utils"] = utils_package

    for module_name, relative_path in (
        ("s3_log_extraction._regex", "_regex.py"),
        ("s3_log_extraction.utils.encryption", "utils/encryption.py"),
        ("s3_log_extraction.validate._base_validator", "validate/_base_validator.py"),
        (
            "s3_log_extraction.validate._extraction_heuristic_pre_validator",
            "validate/_extraction_heuristic_pre_validator.py",
        ),
    ):
        spec = importlib.util.spec_from_file_location(module_name, source_root / relative_path)
        module = importlib.util.module_from_spec(spec)
        assert spec is not None and spec.loader is not None
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

    return sys.modules["s3_log_extraction.validate._extraction_heuristic_pre_validator"]


def test_drogon_ip_regex_uses_encryption_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_extraction_heuristic_module()
    monkeypatch.delenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", raising=False)
    monkeypatch.setattr(module, "decrypt_bytes", lambda *, encrypted_data: b"decoded-regex")

    assert module._get_drogon_ip_regex() == "decoded-regex"


def test_drogon_ip_regex_uses_plaintext_override_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_extraction_heuristic_module()
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.setenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", "plain-regex")
    monkeypatch.setattr(module, "decrypt_bytes", lambda *, encrypted_data: b"should-not-be-used")

    assert module._get_drogon_ip_regex() == "plain-regex"


def test_drogon_ip_regex_requires_plaintext_value_when_encryption_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_extraction_heuristic_module()
    monkeypatch.setenv("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "false")
    monkeypatch.delenv("S3_LOG_EXTRACTION_DROGON_IP_REGEX", raising=False)

    with pytest.raises(EnvironmentError, match="S3_LOG_EXTRACTION_DROGON_IP_REGEX"):
        module._get_drogon_ip_regex()

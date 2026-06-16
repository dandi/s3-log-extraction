"""Tests for the encryption utilities, including password-strength enforcement."""

import pathlib
import secrets
import uuid

import cryptography.fernet
import pytest

import s3_log_extraction

# A randomly generated, high-entropy secret of the kind that should always be accepted.
_STRONG_PASSWORD = secrets.token_urlsafe(32)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "generated_password",
    [
        secrets.token_urlsafe(32),
        secrets.token_urlsafe(16),
        secrets.token_hex(16),
        uuid.uuid4().hex,
    ],
)
def test_validate_password_strength_accepts_generated_secrets(generated_password: str) -> None:
    """Randomly generated high-entropy secrets should pass validation without raising."""
    s3_log_extraction.utils.validate_password_strength(generated_password)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "weak_password",
    [
        "hunter2",  # too short
        "Password123!",  # too short (12 characters)
        "aaaaaaaaaaaaaaaaaaaa",  # long but only one distinct character
        "12345678901234567890",  # long but only digits, so low entropy
    ],
)
def test_validate_password_strength_rejects_weak_passwords(weak_password: str) -> None:
    """Short or low-variety human-chosen passwords should be rejected."""
    with pytest.raises(ValueError, match="not strong enough"):
        s3_log_extraction.utils.validate_password_strength(weak_password)


@pytest.mark.ai_generated
def test_validate_password_strength_error_mentions_env_var_and_recommendation() -> None:
    """The error message should name the environment variable and suggest a generated secret."""
    with pytest.raises(ValueError) as error_info:
        s3_log_extraction.utils.validate_password_strength("short")

    message = str(error_info.value)
    assert ("S3_LOG_EXTRACTION_PASSWORD" in message) is True
    assert ("secrets.token_urlsafe" in message) is True


@pytest.mark.ai_generated
def test_get_key_requires_environment_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_key should raise EnvironmentError when the password variable is unset."""
    monkeypatch.delenv("S3_LOG_EXTRACTION_PASSWORD", raising=False)

    with pytest.raises(EnvironmentError, match="S3_LOG_EXTRACTION_PASSWORD"):
        s3_log_extraction.utils.get_key()


@pytest.mark.ai_generated
def test_get_key_rejects_weak_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_key should refuse to derive a key from a weak password before any encryption happens."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", "hunter2")

    with pytest.raises(ValueError, match="not strong enough"):
        s3_log_extraction.utils.get_key()


@pytest.mark.ai_generated
def test_get_key_returns_valid_deterministic_fernet_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_key should return a valid Fernet key that is stable across calls for the same password."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)
    monkeypatch.delenv("S3_LOG_EXTRACTION_SALT", raising=False)

    key = s3_log_extraction.utils.get_key()

    # Must be accepted by Fernet, which validates the length and base64url format.
    cryptography.fernet.Fernet(key=key)
    # Determinism is required so that data encrypted in one run can be decrypted in another.
    assert s3_log_extraction.utils.get_key() == key


@pytest.mark.ai_generated
def test_get_key_salt_override_changes_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Overriding the salt should produce a different key for the same password."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    monkeypatch.delenv("S3_LOG_EXTRACTION_SALT", raising=False)
    default_salt_key = s3_log_extraction.utils.get_key()

    monkeypatch.setenv("S3_LOG_EXTRACTION_SALT", "a-different-salt-value")
    custom_salt_key = s3_log_extraction.utils.get_key()

    assert (default_salt_key != custom_salt_key) is True


@pytest.mark.ai_generated
def test_encrypt_decrypt_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bytes encrypted with a strong password should decrypt back to the original."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    data = b"192.0.2.1\n198.51.100.2\n"
    encrypted_data = s3_log_extraction.utils.encrypt_bytes(data=data)

    assert (encrypted_data != data) is True
    assert s3_log_extraction.utils.decrypt_bytes(encrypted_data=encrypted_data) == data


@pytest.mark.ai_generated
def test_write_then_read_encrypted_file_round_trip(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Text written with encryption should be unreadable as plaintext but recoverable via decryption."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    file_path = tmp_path / "ips.txt"
    text = "192.0.2.1\n198.51.100.2\n"

    s3_log_extraction.utils.write_text_to_file(file_path=file_path, text=text, use_encryption=True)

    # The on-disk bytes should be ciphertext, not the original plaintext.
    assert (file_path.read_bytes() != text.encode(encoding="utf-8")) is True
    assert s3_log_extraction.utils.read_text_from_file(file_path=file_path, use_encryption=True) == text


@pytest.mark.ai_generated
def test_read_empty_encrypted_file_returns_empty_string(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reading an empty file with encryption enabled should return an empty string rather than error."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    file_path = tmp_path / "empty.txt"
    file_path.write_bytes(b"")

    assert s3_log_extraction.utils.read_text_from_file(file_path=file_path, use_encryption=True) == ""


@pytest.mark.ai_generated
def test_write_then_read_plaintext_file_round_trip(tmp_path: pathlib.Path) -> None:
    """With encryption disabled, content should be written and read back as plaintext."""
    file_path = tmp_path / "plain.txt"
    text = "192.0.2.1\n"

    s3_log_extraction.utils.write_text_to_file(file_path=file_path, text=text, use_encryption=False)

    assert file_path.read_text() == text
    assert s3_log_extraction.utils.read_text_from_file(file_path=file_path, use_encryption=False) == text

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
    "password, is_strong",
    [
        (secrets.token_urlsafe(32), True),
        (secrets.token_urlsafe(16), True),
        (secrets.token_hex(16), True),
        (uuid.uuid4().hex, True),
        ("café-" + secrets.token_urlsafe(24), True),  # strong, and includes a non-ASCII character
        ("hunter2", False),  # too short
        ("Password123!", False),  # too short (12 characters)
        ("aaaaaaaaaaaaaaaaaaaa", False),  # long but only one distinct character
        ("12345678901234567890", False),  # long but only digits, so low entropy
        ("", False),  # empty password has zero estimated entropy
    ],
)
def test_validate_password_strength(password: str, is_strong: bool) -> None:
    """Generated secrets pass validation while weak human-chosen passwords are rejected."""
    if is_strong:
        s3_log_extraction.utils.validate_password_strength(password)
        return

    with pytest.raises(ValueError, match="not strong enough") as error_info:
        s3_log_extraction.utils.validate_password_strength(password)

    # The error should name the environment variable and recommend a generated secret.
    message = str(error_info.value)
    assert ("S3_LOG_EXTRACTION_PASSWORD" in message) is True
    assert ("secrets.token_urlsafe" in message) is True


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "password, expected_error",
    [
        (None, EnvironmentError),  # environment variable unset
        ("hunter2", ValueError),  # password too weak
        (_STRONG_PASSWORD, None),  # accepted
    ],
)
def test_get_key_validates_before_deriving(
    password: str | None, expected_error: type[Exception] | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """get_key requires the env var, rejects weak passwords, and otherwise returns a deterministic Fernet key."""
    monkeypatch.delenv("S3_LOG_EXTRACTION_SALT", raising=False)
    if password is None:
        monkeypatch.delenv("S3_LOG_EXTRACTION_PASSWORD", raising=False)
    else:
        monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", password)

    if expected_error is not None:
        with pytest.raises(expected_error):
            s3_log_extraction.utils.get_key()
        return

    key = s3_log_extraction.utils.get_key()
    cryptography.fernet.Fernet(key=key)  # Validates the length and base64url format.
    # Determinism is required so that data encrypted in one run can be decrypted in another.
    assert s3_log_extraction.utils.get_key() == key


@pytest.mark.ai_generated
def test_get_key_salt_override_changes_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Overriding the salt produces a different key for the same password."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    monkeypatch.delenv("S3_LOG_EXTRACTION_SALT", raising=False)
    default_salt_key = s3_log_extraction.utils.get_key()

    monkeypatch.setenv("S3_LOG_EXTRACTION_SALT", "a-different-salt-value")
    assert (s3_log_extraction.utils.get_key() != default_salt_key) is True


@pytest.mark.ai_generated
@pytest.mark.parametrize("data", [b"192.0.2.1\n198.51.100.2\n", b""])
def test_encrypt_decrypt_round_trip(data: bytes, monkeypatch: pytest.MonkeyPatch) -> None:
    """Bytes encrypted with a strong password decrypt back to the original."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)

    encrypted_data = s3_log_extraction.utils.encrypt_bytes(data=data)

    assert s3_log_extraction.utils.decrypt_bytes(encrypted_data=encrypted_data) == data


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    "text, use_encryption",
    [
        ("192.0.2.1\n198.51.100.2\n", True),  # encrypted round trip
        ("192.0.2.1\n", False),  # plaintext round trip
        ("", True),  # empty on-disk file should decrypt to an empty string
    ],
)
def test_file_round_trip(
    text: str, use_encryption: bool, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Text written to a file is recovered on read, and encrypted content is never stored as plaintext."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", _STRONG_PASSWORD)
    file_path = tmp_path / "ips.txt"

    if text == "":
        # Represents an empty cache file, exercising the empty-file early return on read.
        file_path.write_bytes(b"")
    else:
        s3_log_extraction.utils.write_text_to_file(file_path=file_path, text=text, use_encryption=use_encryption)
        if use_encryption:
            # The on-disk bytes should be ciphertext, not the original plaintext.
            assert (file_path.read_bytes() != text.encode(encoding="utf-8")) is True

    assert s3_log_extraction.utils.read_text_from_file(file_path=file_path, use_encryption=use_encryption) == text

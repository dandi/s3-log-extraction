import base64
import math
import os
import pathlib
import string

import cryptography.fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

# Default salt used when `S3_LOG_EXTRACTION_SALT` is not set.
# The key derivation must be deterministic so that data encrypted in one run can be decrypted in another,
# which means the salt has to be stable rather than randomly generated per call.
# It can be overridden with the `S3_LOG_EXTRACTION_SALT` environment variable for stronger separation.
_DEFAULT_SALT = b"s3_log_extraction"

# Number of PBKDF2 iterations; follows the OWASP recommendation for PBKDF2-HMAC-SHA256.
_KDF_ITERATIONS = 600_000

# Minimum password requirements. These are tuned to comfortably accept randomly generated high-entropy
# secrets (such as `secrets.token_urlsafe`, `secrets.token_hex`, or UUIDs) while rejecting short or
# low-variety human-chosen passwords. A password is the only secret protecting encrypted data at rest,
# so it must not be brute-forceable.
_MINIMUM_PASSWORD_LENGTH = 16
_MINIMUM_DISTINCT_CHARACTERS = 8
_MINIMUM_ENTROPY_BITS = 90.0


def _estimate_entropy_bits(password: str) -> float:
    """Estimate the entropy of a password in bits from its length and the character classes it uses."""
    charset_size = 0
    if any(character in string.ascii_lowercase for character in password):
        charset_size += 26
    if any(character in string.ascii_uppercase for character in password):
        charset_size += 26
    if any(character in string.digits for character in password):
        charset_size += 10
    if any(character in string.punctuation for character in password):
        charset_size += len(string.punctuation)
    if any(character not in string.printable for character in password):
        charset_size += 100  # Conservative allowance for non-ASCII (e.g. unicode) characters.

    if charset_size == 0:
        return 0.0
    return len(password) * math.log2(charset_size)


def validate_password_strength(password: str) -> None:
    """Raise a ``ValueError`` if the password is too weak to be used for encryption.

    The checks are intentionally heuristic: they enforce a minimum length, a minimum number of distinct
    characters, and a minimum estimated entropy. They are designed to block weak human-chosen passwords
    while accepting randomly generated secrets. They cannot detect dictionary words embedded in an
    otherwise long string, so a randomly generated secret is always recommended.
    """
    problems = []
    if len(password) < _MINIMUM_PASSWORD_LENGTH:
        problems.append(f"it must be at least {_MINIMUM_PASSWORD_LENGTH} characters long (got {len(password)})")
    distinct_characters = len(set(password))
    if distinct_characters < _MINIMUM_DISTINCT_CHARACTERS:
        problems.append(
            f"it must contain at least {_MINIMUM_DISTINCT_CHARACTERS} distinct characters (got {distinct_characters})"
        )
    entropy_bits = _estimate_entropy_bits(password)
    if entropy_bits < _MINIMUM_ENTROPY_BITS:
        problems.append(
            f"its estimated entropy must be at least {_MINIMUM_ENTROPY_BITS:.0f} bits (got {entropy_bits:.0f})"
        )

    if problems:
        joined_problems = "; ".join(problems)
        message = (
            "The value of the `S3_LOG_EXTRACTION_PASSWORD` environment variable is not strong enough: "
            f"{joined_problems}. "
            "Please use a randomly generated secret, for example the output of `python -c \"import secrets; "
            'print(secrets.token_urlsafe(32))"`.'
        )
        raise ValueError(message)


def get_key() -> bytes:
    """Parse the full byte key for the given password.

    The key is derived from the `S3_LOG_EXTRACTION_PASSWORD` environment variable using PBKDF2-HMAC-SHA256,
    a deliberately expensive key derivation function that is resistant to brute-force attacks.
    The salt may be customized via the `S3_LOG_EXTRACTION_SALT` environment variable.

    The password is validated against minimum strength requirements before use; weak passwords are rejected.
    """
    password = os.environ.get("S3_LOG_EXTRACTION_PASSWORD", None)
    if password is None:
        message = "Environment variable `S3_LOG_EXTRACTION_PASSWORD` is not set - unable to run encryption tools."
        raise EnvironmentError(message)
    validate_password_strength(password=password)

    salt = os.environ.get("S3_LOG_EXTRACTION_SALT", None)
    salt_bytes = salt.encode(encoding="utf-8") if salt is not None else _DEFAULT_SALT

    password_bytes = password.encode(encoding="utf-8")
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt_bytes, iterations=_KDF_ITERATIONS)
    derived_key = kdf.derive(key_material=password_bytes)

    key = base64.urlsafe_b64encode(derived_key)
    return key


def encrypt_bytes(data: bytes) -> bytes:
    """
    Encrypt bytes using Fernet symmetric encryption.

    Parameters
    ----------
    data : bytes
        The plaintext bytes to encrypt.

    Returns
    -------
    bytes
        The encrypted bytes.
    """
    key = get_key()
    fernet = cryptography.fernet.Fernet(key=key)

    encrypted_data = fernet.encrypt(data=data)
    return encrypted_data


def decrypt_bytes(encrypted_data: bytes) -> bytes:
    """
    Decrypt bytes using Fernet symmetric encryption.

    Parameters
    ----------
    encrypted_data : bytes
        The encrypted bytes to decrypt.

    Returns
    -------
    bytes
        The decrypted plaintext bytes.
    """
    key = get_key()
    fernet = cryptography.fernet.Fernet(key=key)

    decrypted_data = fernet.decrypt(token=encrypted_data)
    return decrypted_data


def read_text_from_file(*, file_path: pathlib.Path, use_encryption: bool) -> str:
    """Read text from a file, optionally decrypting its contents.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the file to read.
    use_encryption : bool
        If ``True``, the file content is decrypted before returning.
        If ``False``, the file content is read as plaintext.
        Returns an empty string if the file is empty when decryption is requested.
    """
    if use_encryption:
        raw_bytes = file_path.read_bytes()
        if not raw_bytes.strip():
            return ""
        return decrypt_bytes(raw_bytes).decode(encoding="utf-8")
    return file_path.read_text()


def write_text_to_file(*, file_path: pathlib.Path, text: str, use_encryption: bool) -> None:
    """Write text to a file, optionally encrypting its contents.

    Parameters
    ----------
    file_path : pathlib.Path
        Path to the file to write.
    text : str
        The text content to write.
    use_encryption : bool
        If ``True``, the content is encrypted before writing.
        If ``False``, the content is written as plaintext.
    """
    if use_encryption:
        file_path.write_bytes(encrypt_bytes(text.encode(encoding="utf-8")))
    else:
        file_path.write_text(text)


__all__ = [
    "decrypt_bytes",
    "encrypt_bytes",
    "get_key",
    "read_text_from_file",
    "validate_password_strength",
    "write_text_to_file",
]

import base64
import os
import pathlib

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


def get_key() -> bytes:
    """Parse the full byte key for the given password.

    The key is derived from the `S3_LOG_EXTRACTION_PASSWORD` environment variable using PBKDF2-HMAC-SHA256,
    a deliberately expensive key derivation function that is resistant to brute-force attacks.
    The salt may be customized via the `S3_LOG_EXTRACTION_SALT` environment variable.
    """
    password = os.environ.get("S3_LOG_EXTRACTION_PASSWORD", None)
    if password is None:
        message = "Environment variable `S3_LOG_EXTRACTION_PASSWORD` is not set - unable to run encryption tools."
        raise EnvironmentError(message)

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
    "write_text_to_file",
]

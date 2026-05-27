import base64
import hashlib
import os
import pathlib

import cryptography.fernet


def get_key() -> bytes:
    """Parse the full byte key for the given password."""
    password = os.environ.get("S3_LOG_EXTRACTION_PASSWORD", None)
    if password is None:
        message = "Environment variable `S3_LOG_EXTRACTION_PASSWORD` is not set - unable to run encryption tools."
        raise EnvironmentError(message)

    password_bytes = password.encode(encoding="utf-8")
    hexcode = hashlib.sha256(password_bytes).digest()

    key = base64.urlsafe_b64encode(hexcode)
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

import base64
import hashlib
import os

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
    key = get_key()
    fernet = cryptography.fernet.Fernet(key=key)

    encrypted_data = fernet.encrypt(data=data)
    return encrypted_data


def decrypt_bytes(encrypted_data: bytes) -> bytes:
    key = get_key()
    fernet = cryptography.fernet.Fernet(key=key)

    decrypted_data = fernet.decrypt(token=encrypted_data)
    return decrypted_data

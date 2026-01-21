import pathlib
import typing

import yaml

from ..config import get_ip_cache_directory
from ..encryption_utils import decrypt_bytes, encrypt_bytes


def load_index_to_ip(cache_directory: str | pathlib.Path | None = None) -> dict[int, str]:
    """
    Load the index to IP cache from the cache directory.

    Parameters
    ----------
    cache_directory : str | pathlib.Path | None
        Path to the cache directory.
        If `None`, the default cache directory will be used.

    Returns
    -------
    dict[int, str]
        A dictionary mapping indexes to full IP addresses.
    """
    ips_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)
    ips_index_cache_file_path = ips_cache_directory / "indexed_ips.yaml"

    if not ips_index_cache_file_path.exists():
        empty_encrypted_content = encrypt_bytes(data=b"")
        with ips_index_cache_file_path.open("wb") as file_stream:
            file_stream.write(empty_encrypted_content)
        return {}

    encrypted_content = ips_index_cache_file_path.read_bytes()
    decrypted_content = decrypt_bytes(encrypted_data=encrypted_content)

    index_to_ip = yaml.safe_load(stream=decrypted_content) or {}
    return index_to_ip


def save_index_to_ip(*, index_to_ip: dict[int, str], cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Save the index to IP cache to the cache directory.

    Parameters
    ----------
    index_to_ip : dict[int, str]
        A dictionary mapping indexes to full IP addresses.
    """
    ip_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)
    ip_index_cache_file_path = ip_cache_directory / "indexed_ips.yaml"

    data = yaml.dump(data=index_to_ip).encode(encoding="utf-8")
    encrypted_content = encrypt_bytes(data=data)
    ip_index_cache_file_path.write_bytes(data=encrypted_content)


def load_ip_cache(
    *,
    cache_type: typing.Literal["index_to_region", "index_not_in_services"],
) -> dict[int, str]:
    """Load the index to region cache from the cache directory."""
    ip_cache_directory = get_ip_cache_directory()
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    if not cache_file_path.exists():
        cache_file_path.touch()
        return {}

    with cache_file_path.open(mode="r") as file_stream:
        content = file_stream.read()

    data = yaml.safe_load(stream=content) or {}
    return data

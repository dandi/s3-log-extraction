import yaml

from ..config import get_cache_directory
from ..encryption_utils import decrypt_bytes, encrypt_bytes


def load_index_to_ip() -> dict[int, str]:
    """
    Load the index to IP cache from the cache directory.

    Returns
    -------
    dict[int, str]
        A dictionary mapping indices to full IP addresses.
    """
    ips_cache_directory = get_cache_directory() / "ips"
    ips_cache_directory.mkdir(exist_ok=True)
    ips_index_cache_file_path = ips_cache_directory / "indexed_ips.yaml"

    if not ips_index_cache_file_path.exists():
        ips_index_cache_file_path.touch()
        return {}

    with ips_index_cache_file_path.open(mode="rb") as file_stream:
        encrypted_content = file_stream.read()

    decrypted_content = decrypt_bytes(encrypted_data=encrypted_content)

    index_to_ip = yaml.safe_load(stream=decrypted_content) or {}
    return index_to_ip


def save_index_to_ip(index_to_ip: dict[int, str]) -> None:
    """
    Save the index to IP cache to the cache directory.

    Parameters
    ----------
    index_to_ip : dict[int, str]
        A dictionary mapping indices to full IP addresses.
    """
    ips_cache_directory = get_cache_directory() / "ips"
    ips_cache_directory.mkdir(exist_ok=True)
    ips_index_cache_file_path = ips_cache_directory / "indexed_ips.yaml"

    data = yaml.dump(data=index_to_ip).encode(encoding="utf-8")
    encrypted_content = encrypt_bytes(data=data)

    with ips_index_cache_file_path.open(mode="wb") as file_stream:
        file_stream.write(encrypted_content)

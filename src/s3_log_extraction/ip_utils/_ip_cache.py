import pathlib
import typing

import yaml

from ..config import get_ip_cache_directory


def load_ip_cache(
    *,
    cache_type: typing.Literal["ip_to_region", "ip_not_in_services", "region_codes_to_coordinates"],
    cache_directory: str | pathlib.Path | None = None,
    encrypt: bool = True,
) -> dict[str, str]:
    """Load the IP cache from the cache directory.

    Parameters
    ----------
    cache_type : str
        The type of IP cache to load.
    cache_directory : path-like, optional
        The cache directory to use. If ``None``, the default cache directory is used.
    encrypt : bool, optional
        If ``True`` (default), the cache file content is decrypted before parsing.
        If ``False``, the file content is read as plaintext YAML.
    """
    ip_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    if not cache_file_path.exists():
        cache_file_path.touch()
        return {}

    if encrypt:
        from ..utils.encryption import decrypt_bytes

        raw_bytes = cache_file_path.read_bytes()
        if not raw_bytes.strip():
            return {}
        content = decrypt_bytes(raw_bytes).decode(encoding="utf-8")
    else:
        with cache_file_path.open(mode="r") as file_stream:
            content = file_stream.read()

    data = yaml.safe_load(stream=content) or {}
    return data


def _write_ip_cache(
    *,
    data: dict,
    cache_type: typing.Literal["ip_to_region", "ip_not_in_services", "region_codes_to_coordinates"],
    cache_directory: str | pathlib.Path | None = None,
    encrypt: bool = True,
) -> None:
    """Write data to an IP cache file, optionally encrypting the content.

    Parameters
    ----------
    data : dict
        The data to write to the cache file.
    cache_type : str
        The type of IP cache to write.
    cache_directory : path-like, optional
        The cache directory to use. If ``None``, the default cache directory is used.
    encrypt : bool, optional
        If ``True`` (default), the content is encrypted before writing.
        If ``False``, the content is written as plaintext YAML.
    """
    ip_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    text = yaml.dump(data=data)
    if encrypt:
        from ..utils.encryption import encrypt_bytes

        cache_file_path.write_bytes(encrypt_bytes(text.encode(encoding="utf-8")))
    else:
        with cache_file_path.open(mode="w") as file_stream:
            file_stream.write(text)

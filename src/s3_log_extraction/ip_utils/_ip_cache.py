import pathlib
import typing

import yaml

from ..config import get_ip_cache_directory


def load_ip_cache(
    *,
    cache_type: typing.Literal["ip_to_region", "region_codes_to_coordinates"],
    cache_directory: str | pathlib.Path | None = None,
) -> dict[str, str]:
    """Load the IP cache from the cache directory."""
    ip_cache_directory = get_ip_cache_directory(cache_directory=cache_directory)
    cache_file_path = ip_cache_directory / f"{cache_type}.yaml"

    if not cache_file_path.exists():
        cache_file_path.touch()
        return {}

    with cache_file_path.open(mode="r") as file_stream:
        content = file_stream.read()

    data = yaml.safe_load(stream=content) or {}
    return data

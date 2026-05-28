"""Configuration options, such as cache management."""

from ._config import (
    save_config,
    get_config,
    get_cache_directory,
    get_cache_subdirectory,
    set_cache_directory,
)
from ._globals import (
    S3_LOG_EXTRACTION_BASE_FOLDER_PATH,
    S3_LOG_EXTRACTION_CONFIG_FILE_PATH,
    DEFAULT_CACHE_DIRECTORY,
)
from ._reset import reset_extraction

__all__ = [
    "S3_LOG_EXTRACTION_BASE_FOLDER_PATH",
    "S3_LOG_EXTRACTION_CONFIG_FILE_PATH",
    "DEFAULT_CACHE_DIRECTORY",
    "save_config",
    "get_config",
    "get_cache_directory",
    "get_cache_subdirectory",
    "set_cache_directory",
    "reset_extraction",
]

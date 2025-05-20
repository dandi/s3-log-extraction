import json
import pathlib
import typing

DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH = pathlib.Path.home() / ".s3-log-extractor"
DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH.mkdir(exist_ok=True)

DEFAULT_CACHE_DIRECTORY = pathlib.Path.home() / ".s3-log-extractor-cache"

_IP_HASH_TO_REGION_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip-hash-to-region.yaml"
_IP_HASH_NOT_IN_SERVICES_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "ip-hash-not-in-services.yaml"

DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH = DANDI_S3_LOG_PARSER_BASE_FOLDER_PATH / "config.yaml"

def save_config(config: Dict[str, typing.Any]) -> None:
    """
    Save the configuration for the DANDI S3 log parser.

    Parameters
    ----------
    config : dict
        The configuration for the DANDI S3 log parser.
    """
    # TODO: add basic schema and validation
    if not any(config):
        return

    with open(file=DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH, mode="w") as file_stream:
        json.dump(obj=config, fp=file_stream)

def get_config() -> Dict[str, typing.Any]:
    """
    Get the configuration for the DANDI S3 log parser.

    Returns
    -------
    dict
        The configuration for the DANDI S3 log parser.
    """
    config = {}
    if not DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH.exists():
        with open(file=DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH, mode="w") as file_stream:
            json.dump(obj=config, fp=file_stream)

    with open(file=DANDI_S3_LOG_PARSER_CONFIG_FILE_PATH, mode="r") as file_stream:
        config = json.load(fp=file_stream)

    return config

def set_cache_directory(directory: str | pathlib.Path) -> None:
    cache_directory = pathlib.Path(directory)
    cache_directory.mkdir(exist_ok=True)

    config = get_config()
    config["cache_directory"] = str(directory)
    save_config(config=config)

def get_cache_directory() -> pathlib.Path:
    """
    Get the cache directory for the DANDI S3 log parser.

    Returns
    -------
    pathlib.Path
        The cache directory for the DANDI S3 log parser.
    """
    config = get_config()

    directory = config.get("cache_directory", pathlib.Path.home() / ".dandi_s3_log_cache")
    directory = pathlib.Path(directory)
    directory.mkdir(exist_ok=True)

    return directory


def get_validation_directory() -> pathlib.Path:
    """
    Get the cache directory for the DANDI S3 log parser.

    Returns
    -------
    pathlib.Path
        The cache directory for the DANDI S3 log parser.
    """
    cache_directory = get_cache_directory()

    validation_directory = cache_directory / "validation_records"
    validation_directory.mkdir(exist_ok=True)

    return directory

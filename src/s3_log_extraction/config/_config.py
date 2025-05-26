import json
import pathlib
import typing

from ._globals import DEFAULT_CACHE_DIRECTORY, S3_LOG_EXTRACTION_CONFIG_FILE_PATH


def save_config(config: dict[str, typing.Any]) -> None:
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

    with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
        json.dump(obj=config, fp=file_stream)


def get_config() -> dict[str, typing.Any]:
    """
    Get the configuration for the DANDI S3 log parser.

    Returns
    -------
    dict
        The configuration for the DANDI S3 log parser.
    """
    config = {}
    if not S3_LOG_EXTRACTION_CONFIG_FILE_PATH.exists():
        with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
            json.dump(obj=config, fp=file_stream)

    with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="r") as file_stream:
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

    directory = pathlib.Path(config.get("cache_directory", DEFAULT_CACHE_DIRECTORY))
    directory.mkdir(exist_ok=True)

    return directory


def get_records_directory() -> pathlib.Path:
    """
    Get the records directory for the DANDI S3 log parser.

    Records are ways of tracking the progress of the extraction and validation processes so they do not needlessly
    repeat computations.

    Returns
    -------
    pathlib.Path
        The records directory for the DANDI S3 log parser.
    """
    cache_directory = get_cache_directory()

    records_directory = cache_directory / "records"
    records_directory.mkdir(exist_ok=True)

    return records_directory

import json
import pathlib
import typing

from ._globals import DEFAULT_CACHE_DIRECTORY, S3_LOG_EXTRACTION_CONFIG_FILE_PATH


def save_config(config: dict[str, typing.Any]) -> None:
    """
    Save the configuration for S3 log extraction.

    Parameters
    ----------
    config : dict
        The configuration for S3 log extraction.
    """
    # TODO: add basic schema and validation
    if not any(config):
        return

    with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
        json.dump(obj=config, fp=file_stream, indent=2)


def get_config() -> dict[str, typing.Any]:
    """
    Get the configuration for S3 log extraction.

    Returns
    -------
    dict
        The configuration for S3 log extraction.
    """
    config = {}
    if not S3_LOG_EXTRACTION_CONFIG_FILE_PATH.exists():
        with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
            json.dump(obj=config, fp=file_stream, indent=2)

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
    Get the cache directory for S3 log extraction.

    Returns
    -------
    pathlib.Path
        The base cache directory for S3 log extraction.
    """
    config = get_config()

    directory = pathlib.Path(config.get("cache_directory", DEFAULT_CACHE_DIRECTORY))
    directory.mkdir(exist_ok=True)

    return directory


def get_cache_subdirectory(
    *,
    cache_directory: str | pathlib.Path | None = None,
    name: str,
) -> pathlib.Path:
    """
    Get a named subdirectory of the cache directory, creating it if it does not exist.

    The individual ``get_*_directory`` helpers below are thin wrappers around this function,
    kept for API and docstring exposure and to make it easier to navigate the codebase via
    auto-completion and IDEs.

    Parameters
    ----------
    cache_directory : path-like, optional
        The directory to use as the cache directory.
        If not provided, the default cache directory is used.
    name : str
        The name of the subdirectory to create within the cache directory.

    Returns
    -------
    pathlib.Path
        The named subdirectory of the cache directory.
    """
    cache_dir = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()

    cache_subdir = cache_dir / name
    cache_subdir.mkdir(exist_ok=True)

    return cache_subdir


def get_records_directory(cache_directory: str | pathlib.Path | None = None) -> pathlib.Path:
    """
    Get the records subdirectory for S3 log extraction.

    Records are ways of tracking the progress of the extraction and validation processes so they do not needlessly
    repeat computations.

    Parameters
    ----------
    cache_directory : path-like, optional
        The directory to use as the cache directory.
        If not provided, the default cache directory is used.

    Returns
    -------
    pathlib.Path
        The main output directory for S3 log extraction.
    """
    return get_cache_subdirectory(cache_directory=cache_directory, name="records")


def get_ip_cache_directory(cache_directory: str | pathlib.Path | None = None) -> pathlib.Path:
    """
    Get the IP cache subdirectory for S3 log extraction.

    Records are ways of tracking the progress of the extraction and validation processes so they do not needlessly
    repeat computations.

    Parameters
    ----------
    cache_directory : path-like, optional
        The directory to use as the cache directory.
        If not provided, the default cache directory is used.


    Returns
    -------
    pathlib.Path
        The IP cache directory for S3 log extraction.
    """
    return get_cache_subdirectory(cache_directory=cache_directory, name="ips")


def get_summary_directory(cache_directory: str | pathlib.Path | None = None) -> pathlib.Path:
    """
    Get the summary subdirectory for S3 log extraction.

    Parameters
    ----------
    cache_directory : path-like, optional
        The directory to use as the cache directory.
        If not provided, the default cache directory is used.

    Returns
    -------
    pathlib.Path
        The summary directory for S3 log extraction.
    """
    return get_cache_subdirectory(cache_directory=cache_directory, name="summaries")

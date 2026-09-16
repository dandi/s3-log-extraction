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
    # An empty mapping is written out rather than skipped, so that removing the last remaining
    # setting actually clears it from the file instead of silently leaving the old value in place.
    with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
        json.dump(obj=config, fp=file_stream, indent=2, sort_keys=True)


def get_config() -> dict[str, typing.Any]:
    """
    Get the configuration for S3 log extraction.

    Returns
    -------
    dict
        The configuration for S3 log extraction.
    """
    config: dict[str, typing.Any] = {}
    if not S3_LOG_EXTRACTION_CONFIG_FILE_PATH.exists():
        with open(file=S3_LOG_EXTRACTION_CONFIG_FILE_PATH, mode="w") as file_stream:
            json.dump(obj=config, fp=file_stream, indent=2, sort_keys=True)

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


def set_scratch_directory(directory: str | pathlib.Path, /) -> None:
    """
    Set the master scratch directory that extraction runs create their working directories beneath.

    Parameters
    ----------
    directory : path-like
        The directory to create per-run working directories under.
        Extraction writes worker output here before merging it into the cache, so it needs free space
        on the order of one batch of extracted logs and it should be on a fast local disk.
    """
    scratch_directory = pathlib.Path(directory)
    scratch_directory.mkdir(parents=True, exist_ok=True)

    config = get_config()
    config["scratch_directory"] = str(scratch_directory)
    save_config(config=config)


def unset_scratch_directory() -> None:
    """Remove any configured master scratch directory, restoring use of the system temporary directory."""
    config = get_config()
    config.pop("scratch_directory", None)
    save_config(config=config)


def get_scratch_directory() -> pathlib.Path | None:
    """
    Get the master scratch directory that extraction runs create their working directories beneath.

    Returns
    -------
    pathlib.Path | None
        The configured master scratch directory, or `None` if none is configured.
        `None` means the system temporary directory is used, as selected by `TMPDIR` or the platform default.
    """
    config = get_config()

    configured_directory = config.get("scratch_directory", None)
    if configured_directory is None:
        return None

    scratch_directory = pathlib.Path(configured_directory)
    scratch_directory.mkdir(parents=True, exist_ok=True)

    return scratch_directory


def get_cache_subdirectory(
    *,
    cache_directory: str | pathlib.Path | None = None,
    name: str,
) -> pathlib.Path:
    """
    Get a named subdirectory of the cache directory, creating it if it does not exist.


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

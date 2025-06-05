import json
import pathlib
import sys
import typing
import warnings

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
        json.dump(obj=config, fp=file_stream)


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


def set_awk_path(file_path: str | pathlib.Path) -> None:
    if sys.platform != "win32":
        message = "Setting the AWK path is only applicable to Windows systems - ignoring set command."
        warnings.warn(message=message)
        return
    file_path = pathlib.Path(file_path)

    config = get_config()
    config["awk_path"] = str(file_path.absolute())
    save_config(config=config)


def get_awk_path() -> str | pathlib.Path:
    """
    Get the location of the AWK executable for S3 log extraction.

    Only applies to Windows systems.

    Returns
    -------
    pathlib.Path
        The path to the AWK executable for S3 log extraction.
    """
    if sys.platform != "win32":
        return "awk"

    config = get_config()

    default_awk_path = pathlib.Path.home() / "anaconda3" / "Library" / "usr" / "bin" / "awk.exe"
    awk_path = pathlib.Path(config.get("awk_path", default_awk_path))
    if not awk_path.exists():
        message = (
            "\nUnable to find `awk`, which is required for extraction - "
            "please set this using `s3_log_extraction.set_awk_path(file_path=...)`.\n\n"
        )
        raise RuntimeError(message)

    return awk_path


def get_records_directory(*, cache_directory: str | pathlib.Path | None = None) -> pathlib.Path:
    """
    Get the records directory for S3 log extraction.

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
        The records directory for S3 log extraction.
    """
    cache_directory = cache_directory or get_cache_directory()

    records_directory = cache_directory / "records"
    records_directory.mkdir(exist_ok=True)

    return records_directory


def get_ip_cache_directory() -> pathlib.Path:
    """
    Get the IP cache directory for S3 log extraction.

    Records are ways of tracking the progress of the extraction and validation processes so they do not needlessly
    repeat computations.

    Returns
    -------
    pathlib.Path
        The IP cache directory for S3 log extraction.
    """
    cache_directory = get_cache_directory()

    ip_cache_directory = cache_directory / "ips"
    ip_cache_directory.mkdir(exist_ok=True)

    return ip_cache_directory

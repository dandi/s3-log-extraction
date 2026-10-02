import collections.abc
import ipaddress
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


def set_base_temporary_directory(directory: str | pathlib.Path, /) -> None:
    """
    Set the base directory that extraction runs create their temporary directories inside.

    Parameters
    ----------
    directory : path-like
        The directory to use in place of the system temporary directory.
        Extraction writes worker output here before merging it into the cache, so it needs free space
        on the order of one batch of extracted logs and it should be on a fast local disk.
    """
    base_temporary_directory = pathlib.Path(directory)
    base_temporary_directory.mkdir(parents=True, exist_ok=True)

    config = get_config()
    config["base_temporary_directory"] = str(base_temporary_directory)
    save_config(config=config)


def unset_base_temporary_directory() -> None:
    """Remove any configured base temporary directory, restoring use of the system temporary directory."""
    config = get_config()
    config.pop("base_temporary_directory", None)
    save_config(config=config)


def get_base_temporary_directory() -> pathlib.Path | None:
    """
    Get the base directory that extraction runs create their temporary directories inside.

    Returns
    -------
    pathlib.Path | None
        The configured base temporary directory, or `None` if none is configured.
        `None` means the system temporary directory is used, as selected by `TMPDIR` or the platform default.
    """
    config = get_config()

    configured_directory = config.get("base_temporary_directory", None)
    if configured_directory is None:
        return None

    base_temporary_directory = pathlib.Path(configured_directory)
    base_temporary_directory.mkdir(parents=True, exist_ok=True)

    return base_temporary_directory


def set_excluded_ips(ips: collections.abc.Iterable[str], /) -> None:
    """
    Set the IP addresses whose activity is left out of the published view and requester counts.

    The list replaces any previously configured one. It is stored only in the local configuration file,
    so the addresses never enter the published summaries or the repository.

    Parameters
    ----------
    ips : iterable of str
        The individual IPv4 or IPv6 addresses to exclude. Each is stored in its canonical text form,
        which is the form the extraction cache records. Networks in CIDR notation are not accepted.
        An empty iterable removes the setting, the same as ``unset_excluded_ips``.

    Raises
    ------
    ValueError
        If any entry is not a single valid IP address.
    """
    canonical_ips = set()
    for ip in ips:
        try:
            canonical_ips.add(str(ipaddress.ip_address(ip.strip())))
        except ValueError as exception:
            message = (
                f"\n\nThe excluded IP entry '{ip}' is not a single valid IP address.\n"
                "Only individual addresses can be excluded. Networks in CIDR notation are not accepted.\n\n"
            )
            raise ValueError(message) from exception

    config = get_config()
    if canonical_ips:
        config["excluded_ips"] = sorted(canonical_ips)
    else:
        config.pop("excluded_ips", None)
    save_config(config=config)


def unset_excluded_ips() -> None:
    """Remove any configured excluded IP addresses, so that every requester is counted again."""
    config = get_config()
    config.pop("excluded_ips", None)
    save_config(config=config)


def get_excluded_ips() -> frozenset[str]:
    """
    Get the IP addresses whose activity is left out of the published view and requester counts.

    Returns
    -------
    frozenset of str
        The configured addresses in canonical text form.
        An empty set when none are configured, in which case no requester is excluded by address.
    """
    config = get_config()

    excluded_ips = frozenset(config.get("excluded_ips", []))
    return excluded_ips


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

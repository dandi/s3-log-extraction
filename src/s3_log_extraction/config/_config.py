import collections.abc
import ipaddress
import json
import os
import pathlib
import typing

import dotenv

from ._globals import (
    DEFAULT_CACHE_DIRECTORY,
    DOTENV_FILE_PATH,
    EXCLUDED_IPS_ENVIRONMENT_VARIABLE,
    EXCLUDED_IPS_FILE_PATH,
    S3_LOG_EXTRACTION_CONFIG_FILE_PATH,
)


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


def get_excluded_ips(extra_ips: collections.abc.Iterable[str] | None = None, /) -> frozenset[str]:
    """
    Get the IP addresses whose requests are dropped at extraction and left out of every published summary.

    The set is the union of three sources.

    1. The file ``EXCLUDED_IPS_FILE_PATH`` (``~/.s3-log-extraction/excluded_ips.txt``), which is edited by hand
       and never written by this package. It holds one IPv4 or IPv6 address per line. Blank lines are ignored,
       as is anything after a ``#``, so each entry can carry a note on why it is excluded.
    2. The environment variable ``S3_LOG_EXTRACTION_EXCLUDED_IPS``, holding addresses separated by commas or
       whitespace. When it is not set in the environment, it is read from the dotenv file ``DOTENV_FILE_PATH``
       (``~/.s3-log-extraction/.env``) if that exists. A variable already set in the environment always wins.
    3. The ``extra_ips`` given here, such as those passed to the ``--exclude`` option on the command line.

    Every entry must be a single address. Networks in CIDR notation and patterns are not accepted.

    Parameters
    ----------
    extra_ips : iterable of str, optional
        Further addresses to exclude on top of the file and the environment variable.

    Returns
    -------
    frozenset of str
        The addresses in canonical text form, which is the form the extraction cache records.
        An empty set when no source lists an address, in which case no requester is excluded by address.

    Raises
    ------
    ValueError
        If any entry of any source is something other than a single valid IP address.
    """
    canonical_ips = set()

    if EXCLUDED_IPS_FILE_PATH.exists():
        for line_number, line in enumerate(EXCLUDED_IPS_FILE_PATH.read_text().splitlines(), start=1):
            entry = line.split("#", 1)[0].strip()
            if not entry:
                continue
            try:
                canonical_ips.add(str(ipaddress.ip_address(entry)))
            except ValueError as exception:
                message = (
                    f"\n\nLine {line_number} of '{EXCLUDED_IPS_FILE_PATH}' is not a single valid IP address.\n"
                    "Each line must hold one IPv4 or IPv6 address, optionally followed by a '#' comment. "
                    "Networks in CIDR notation are not accepted.\n\n"
                )
                raise ValueError(message) from exception

    environment_value = os.environ.get(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, None)
    if environment_value is None and DOTENV_FILE_PATH.exists():
        environment_value = dotenv.dotenv_values(dotenv_path=DOTENV_FILE_PATH).get(EXCLUDED_IPS_ENVIRONMENT_VARIABLE)
    environment_entries = (environment_value or "").replace(",", " ").split()

    sources = [
        (f"the '{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}' environment variable", environment_entries),
        ("the extra excluded IPs", [entry.strip() for entry in extra_ips or [] if entry.strip()]),
    ]
    for source, entries in sources:
        for entry in entries:
            try:
                canonical_ips.add(str(ipaddress.ip_address(entry)))
            except ValueError as exception:
                message = (
                    f"\n\nThe entry '{entry}' of {source} is not a single valid IP address.\n"
                    "Networks in CIDR notation and patterns are not accepted.\n\n"
                )
                raise ValueError(message) from exception

    excluded_ips = frozenset(canonical_ips)
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

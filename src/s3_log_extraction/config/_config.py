import collections.abc
import ipaddress
import json
import os
import pathlib
import typing

import dotenv

from ._globals import (
    _LEGACY_EXCLUDED_IPS_FILE_PATH,
    DEFAULT_CACHE_DIRECTORY,
    DOTENV_FILE_PATH,
    EXCLUDED_IPS_ENVIRONMENT_VARIABLE,
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

    The addresses come from the environment variable ``S3_LOG_EXTRACTION_EXCLUDED_IPS``, plus any ``extra_ips``.
    When the variable is not set in the environment, it is read with ``python-dotenv`` from the dotenv file
    ``DOTENV_FILE_PATH`` (``~/.s3-log-extraction/.env``), if that exists. A variable already set in the
    environment takes precedence over the file, even when it is empty.

    The recommended layout is a double-quoted value spanning several lines, with one address per line. Anything
    after a ``#`` on a line is a comment, so each address can carry a note on why it is excluded, and blank
    lines are ignored.

    .. code-block:: text

        S3_LOG_EXTRACTION_EXCLUDED_IPS="
        192.0.2.1  # Reviewed mirror
        198.51.100.5
        "

    Commas and whitespace also separate addresses, so a single line such as
    ``S3_LOG_EXTRACTION_EXCLUDED_IPS=192.0.2.1,198.51.100.5`` works too.

    A shell that sources the file (``set -a; . ~/.s3-log-extraction/.env; set +a``) keeps the newlines and the
    ``#`` text of a multi-line value, and the value it exports parses exactly as the file does. A multi-line value
    is not understood by ``docker --env-file``, and appending the file to ``$GITHUB_ENV`` needs GitHub's
    ``NAME<<DELIMITER`` heredoc form. Use the single-line form where those readers matter.

    Every entry must be a single IPv4 or IPv6 address. Networks in CIDR notation and patterns are not accepted.

    Parameters
    ----------
    extra_ips : iterable of str, optional
        Further addresses to exclude on top of the environment variable, such as those passed to the
        ``--exclude`` option on the command line.

    Returns
    -------
    frozenset of str
        The addresses in canonical text form, which is the form the extraction cache records.
        An empty set when no address is listed, in which case no requester is excluded by address.

    Raises
    ------
    ValueError
        If any entry is something other than a single valid IP address, or if the ``excluded_ips.txt`` file
        read by version 1.11.10 still exists.
    """
    if _LEGACY_EXCLUDED_IPS_FILE_PATH.exists():
        message = (
            f"\n\nThe excluded IPs file '{_LEGACY_EXCLUDED_IPS_FILE_PATH}' is no longer read.\n"
            f"Move its lines, comments included, into a double-quoted value in '{DOTENV_FILE_PATH}', then delete "
            "it. This check stops the run so that no excluded requester is silently counted again.\n\n"
            f'{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}="\n'
            "192.0.2.1  # Why it is excluded\n"
            "198.51.100.5\n"
            '"\n\n'
        )
        raise ValueError(message)

    environment_value = os.environ.get(EXCLUDED_IPS_ENVIRONMENT_VARIABLE, None)
    if environment_value is None and DOTENV_FILE_PATH.exists():
        environment_value = dotenv.dotenv_values(dotenv_path=DOTENV_FILE_PATH).get(EXCLUDED_IPS_ENVIRONMENT_VARIABLE)
    environment_entries = [
        entry
        for line in (environment_value or "").splitlines()
        for entry in line.split("#", 1)[0].replace(",", " ").split()
    ]

    sources = [
        (f"'{EXCLUDED_IPS_ENVIRONMENT_VARIABLE}'", environment_entries),
        ("the extra excluded IPs", [entry.strip() for entry in extra_ips or [] if entry.strip()]),
    ]
    canonical_ips = set()
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

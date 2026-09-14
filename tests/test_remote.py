"""
Remote integration tests for IP geolocation.

These tests require real credentials and live network access.
They are marked ``@pytest.mark.remote`` and are run only in the dedicated
remote-testing CI workflow, which supplies valid ``MAXMIND_ACCOUNT_ID`` and
``MAXMIND_LICENSE_KEY`` environment variables.
"""

import os
import pathlib
import re
import shutil

import pytest
import yaml
from conftest import write_by_region_summary

import s3_log_extraction

_AUTH_ERROR_PATTERNS = ("401", "403", "Unknown token", "Unauthorized", "not authorized")

# The status MaxMind answers with once the account's daily download allowance is spent. Pinned here rather than
# imported so that these tests keep their own statement of what the limit looks like.
_DOWNLOAD_QUOTA_EXHAUSTED_STATUS_CODE = 429

# ISO 3166-1 alpha-3 country code, optionally followed by an ISO 3166-2 subdivision code
_REGION_LABEL_PATTERN = re.compile(r"^[A-Z]{3}(/[A-Z0-9]{1,3})?$")


def _is_auth_error(exc: Exception) -> bool:
    """Return True if *exc* looks like an API authentication/authorization failure."""
    exc_str = str(exc).lower()
    return any(pattern.lower() in exc_str for pattern in _AUTH_ERROR_PATTERNS)


def _assert_maxmind_credentials_are_set() -> None:
    for name in ("MAXMIND_ACCOUNT_ID", "MAXMIND_LICENSE_KEY"):
        assert os.environ.get(name, "").strip(), f"{name} environment variable must be set to a non-empty value"


def _skip_if_download_quota_is_spent(exc: Exception) -> None:
    """
    Skip rather than fail when MaxMind refused for a spent daily allowance rather than for cause.

    The allowance belongs to the account and resets on its own, so exhausting it says nothing about the code
    under test. Failing on it turns every run for the rest of the day red, which is what it used to do.
    """
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) != _DOWNLOAD_QUOTA_EXHAUSTED_STATUS_CODE:
        return

    pytest.skip(
        "MaxMind's daily GeoLite2 download allowance for this account is spent, so the database could not be "
        f"obtained and these tests have nothing to geolocate with ({exc}). The allowance resets on its own."
    )


def _fail_if_maxmind_rejected(exc: Exception) -> None:
    if _is_auth_error(exc):
        pytest.fail(
            f"MAXMIND_ACCOUNT_ID and MAXMIND_LICENSE_KEY are set but were rejected by MaxMind ({exc}). "
            "Please verify that the GitHub secrets contain a valid account ID and a license key with GeoLite2 "
            "download permission from https://www.maxmind.com/en/accounts/current/license-key"
        )


@pytest.fixture(scope="session")
def shared_geolite2_database() -> pathlib.Path:
    """
    Resolve the GeoLite2 database once for the whole session, in the configured cache directory.

    Every test here works in its own ``tmp_path``, so each one that reached for the database used to download
    its own copy. A MaxMind account has a daily download allowance, and three copies per run across pull
    request runs and the scheduled run was enough to spend it and turn this workflow red for the rest of the
    day. One copy per session is shared instead, and CI caches the directory it lands in, so a run usually
    downloads nothing at all.

    Resolving here rather than in a temporary directory means a local run populates the developer's own cache
    directory, which is the same place the tool itself uses.
    """
    _assert_maxmind_credentials_are_set()

    try:
        return s3_log_extraction.ip_utils.update_geolite2_database()
    except Exception as exc:
        _skip_if_download_quota_is_spent(exc)
        _fail_if_maxmind_rejected(exc)
        raise


def _seed_geolite2_database(*, cache_directory: pathlib.Path, source_path: pathlib.Path) -> pathlib.Path:
    """Place the session's database where a test's own cache directory expects to find it."""
    destination_path = cache_directory / "geolite2" / source_path.name
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    # copy2 carries the modification time across, so the staleness check still sees the database's true age
    # and a genuinely stale copy is still refreshed rather than silently accepted.
    shutil.copy2(src=source_path, dst=destination_path)
    return destination_path


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_geolite2_database_remote(shared_geolite2_database: pathlib.Path) -> None:
    """
    Test that the GeoLite2-City database is obtainable from MaxMind with the configured credentials.

    The session fixture resolves the database, downloading it whenever the cache directory holds no usable
    copy. The scheduled workflow runs without the CI cache, so the download itself, and with it the check that
    the credentials are still accepted, is exercised at least once a day.

    Parameters
    ----------
    shared_geolite2_database : pathlib.Path
        Path of the database resolved once for the session.
    """
    assert shared_geolite2_database == s3_log_extraction.ip_utils.get_geolite2_database_path()
    assert shared_geolite2_database.exists(), "GeoLite2-City.mmdb is not present"
    assert shared_geolite2_database.stat().st_size > 1_000_000, "GeoLite2-City.mmdb is implausibly small"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_resolver_resolves_public_ip_remote(tmp_path: pathlib.Path, shared_geolite2_database: pathlib.Path) -> None:
    """
    Test that the resolver classifies a real public IP via the live service listings and the local database.

    Uses ``4.4.4.4`` (Level3/Lumen Technologies), a major US-ISP address that is
    outside GitHub, AWS, GCP, and VPN CIDR ranges, to exercise the database lookup path.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    shared_geolite2_database : pathlib.Path
        Path of the database resolved once for the session, seeded into ``tmp_path`` so that this test does not
        spend another of the MaxMind account's daily downloads.
    """
    test_ip = "4.4.4.4"

    _assert_maxmind_credentials_are_set()
    seeded_database_path = _seed_geolite2_database(cache_directory=tmp_path, source_path=shared_geolite2_database)

    try:
        with s3_log_extraction.ip_utils.IpRegionResolver(cache_directory=tmp_path) as resolver:
            region = resolver.resolve(test_ip)
            # The live listings must have been fetched for every known service
            assert set(resolver.service_networks.keys()) == {"GH-actions", "GitHub", "AWS", "GCP", "VPN"}
            assert all(len(networks) > 0 for networks in resolver.service_networks.values())
    except Exception as exc:
        _skip_if_download_quota_is_spent(exc)
        _fail_if_maxmind_rejected(exc)
        raise

    assert isinstance(region, str) and _REGION_LABEL_PATTERN.match(
        region
    ), f"Expected an ISO 3166 label such as 'USA/CA', got: {region!r}"
    assert seeded_database_path.exists(), "The resolver did not read the database from its own cache directory"

    # The resolved label must also have coordinates in the bundled tables, so that the heat maps can place it
    write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=[region])
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text()) or {}
    assert region in coordinates, f"Expected '{region}' to have coordinates, got keys: {list(coordinates.keys())}"
    assert isinstance(coordinates[region]["latitude"], float) and isinstance(coordinates[region]["longitude"], float)


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_region_code_coordinates_locates_aws_region_remote(
    tmp_path: pathlib.Path, shared_geolite2_database: pathlib.Path
) -> None:
    """
    Test that a cloud service region is located with the GeoLite2 database and the live AWS IP range listing.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    shared_geolite2_database : pathlib.Path
        Path of the database resolved once for the session, seeded into ``tmp_path`` so that this test does not
        spend another of the MaxMind account's daily downloads.
    """
    region_code = "AWS/us-east-1"

    _assert_maxmind_credentials_are_set()
    _seed_geolite2_database(cache_directory=tmp_path, source_path=shared_geolite2_database)

    write_by_region_summary(tmp_path / "summaries" / "ds001" / "by_region.tsv", regions=[region_code])

    try:
        s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path, use_encryption=False)
    except Exception as exc:
        _skip_if_download_quota_is_spent(exc)
        _fail_if_maxmind_rejected(exc)
        raise

    coordinates = yaml.safe_load((tmp_path / "ips" / "region_codes_to_coordinates.yaml").read_text()) or {}
    assert region_code in coordinates, f"Expected '{region_code}' to be located, got keys: {list(coordinates.keys())}"
    entry = coordinates[region_code]
    assert isinstance(entry["latitude"], float), f"Expected float latitude, got: {entry['latitude']!r}"
    assert isinstance(entry["longitude"], float), f"Expected float longitude, got: {entry['longitude']!r}"
    # us-east-1 is in Northern Virginia
    assert 24.0 < entry["latitude"] < 50.0 and -125.0 < entry["longitude"] < -66.0, entry

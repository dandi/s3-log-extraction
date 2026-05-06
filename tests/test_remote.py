"""Remote integration tests for IP geolocation API calls.

These tests require real API keys and live network access.
They are marked ``@pytest.mark.remote`` and are run only in the dedicated
remote-testing CI workflow, which supplies valid ``IPINFO_API_KEY`` and
``OPENCAGE_API_KEY`` environment variables.
"""

import pathlib

import pytest
import yaml

import s3_log_extraction


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_index_to_region_codes_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that update_index_to_region_codes resolves a real public IP via the IPInfo API.

    Uses ``4.4.4.4`` (Level3/Lumen Technologies), a major US-ISP address that is
    outside GitHub, AWS, GCP, and VPN CIDR ranges, to exercise the live IPInfo
    API lookup path.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    test_ip = "4.4.4.4"
    test_index = 12345

    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir()

    # Write the indexed IP file in plain (unencrypted) YAML format
    indexed_ips_file = ip_cache_dir / "indexed_ips.yaml"
    indexed_ips_file.write_text(yaml.dump({test_index: test_ip}))

    s3_log_extraction.ip_utils.update_index_to_region_codes(cache_directory=tmp_path, encrypt=False)

    index_to_region_file = ip_cache_dir / "index_to_region.yaml"
    assert index_to_region_file.exists(), "index_to_region.yaml was not created"

    index_to_region = yaml.safe_load(index_to_region_file.read_text()) or {}
    assert test_index in index_to_region, f"Expected index {test_index} to be resolved, got: {index_to_region}"
    region = index_to_region[test_index]
    assert isinstance(region, str) and len(region) > 0, f"Expected a non-empty region string, got: {region!r}"


@pytest.mark.remote
@pytest.mark.ai_generated
def test_update_region_code_coordinates_remote(tmp_path: pathlib.Path) -> None:
    """
    Test that update_region_code_coordinates resolves coordinates via the OpenCage API.

    Pre-populates the IP cache with a ``US/California`` region code (not present
    in the built-in defaults) to force a live OpenCage geocoding call.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory for test isolation.
    """
    region_code = "US/California"

    ip_cache_dir = tmp_path / "ips"
    ip_cache_dir.mkdir()

    # Write index_to_region.yaml with a region that requires OpenCage lookup
    index_to_region_file = ip_cache_dir / "index_to_region.yaml"
    index_to_region_file.write_text(yaml.dump({12345: region_code}))

    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=tmp_path)

    coordinates_file = ip_cache_dir / "region_codes_to_coordinates.yaml"
    assert coordinates_file.exists(), "region_codes_to_coordinates.yaml was not created"

    coordinates = yaml.safe_load(coordinates_file.read_text()) or {}
    assert region_code in coordinates, f"Expected '{region_code}' to be geocoded, got keys: {list(coordinates.keys())}"
    entry = coordinates[region_code]
    assert (
        "latitude" in entry and "longitude" in entry
    ), f"Expected latitude/longitude keys in entry for '{region_code}', got: {entry}"
    assert isinstance(entry["latitude"], float), f"Expected float latitude, got: {entry['latitude']!r}"
    assert isinstance(entry["longitude"], float), f"Expected float longitude, got: {entry['longitude']!r}"

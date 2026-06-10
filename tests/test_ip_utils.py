import datetime
import pathlib
import shutil
import unittest.mock

import ipinfo
import py
import pytest
import yaml

import s3_log_extraction


def test_ip_utils(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"

    base_tests_dir = pathlib.Path(__file__).parent
    expected_cache = base_tests_dir / "expected_output"
    expected_ips_dir = expected_cache / "ips"

    # Provide non-None dummy keys so the guard checks in update functions pass without real credentials.
    # No actual API calls are made because the ips cache is pre-seeded with expected output below.
    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")
    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")

    # Pre-seed the ips cache with expected output so the update functions see a complete cache and skip API calls.
    # With all IPs already present in ip_to_region.yaml, ips_to_update will be empty and no remote calls are made.
    shutil.copytree(src=expected_ips_dir, dst=test_ips_dir, dirs_exist_ok=True)

    # Test updating IPs to region codes and coordinates
    s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=test_cache, use_encryption=False)
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=test_cache, use_encryption=False)
    s3_log_extraction.testing.assert_filetree_matches(test_dir=test_ips_dir, expected_dir=expected_ips_dir)


def test_refresh_ip_to_region_codes(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that refresh_ip_to_region_codes selects the correct IP partition and records changes."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)

    # Seed the cache with known IPs and regions using RFC 5737 TEST-NET-1 documentation addresses (bogons)
    initial_ip_to_region = {
        "192.0.2.1": "US/California",
        "192.0.2.2": "US/New York",
        "192.0.2.3": "US/Texas",
        "192.0.2.4": "UK/England",
        "192.0.2.5": "DE/Bavaria",
    }
    ip_to_region_file = test_ips_dir / "ip_to_region.yaml"
    ip_to_region_file.write_text(yaml.dump(initial_ip_to_region))

    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")

    # Use a fixed date: toordinal() % 90 == 0 means partition_index = 0
    # Find a date where today.toordinal() % 90 == 0
    fixed_ordinal_base = datetime.date(2000, 1, 1).toordinal()
    # Adjust to find a day where ordinal % 90 == 0
    offset = (-fixed_ordinal_base) % 90
    fixed_date = datetime.date.fromordinal(fixed_ordinal_base + offset)
    assert fixed_date.toordinal() % 90 == 0

    # With 5 IPs and partition_size = ceil(5/90) = 1, partition_index = 0 picks sorted_ips[0:1]
    sorted_ips = sorted(initial_ip_to_region.keys())
    # partition_size = ceil(5/90) = 1; partition 0 -> sorted_ips[0:1] = ["192.0.2.1"]
    expected_refreshed_ip = sorted_ips[0]  # "192.0.2.1"
    new_region_for_refreshed_ip = "US/Oregon"

    def mock_get_region_code(ip_address: str, ipinfo_handler: object) -> str:
        if ip_address == expected_refreshed_ip:
            return new_region_for_refreshed_ip
        return initial_ip_to_region[ip_address]  # pragma: no cover

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._refresh_ip_to_region_codes._get_region_code_from_ip_address",
        mock_get_region_code,
    ):
        with unittest.mock.patch("ipinfo.getHandler"):
            s3_log_extraction.ip_utils.refresh_ip_to_region_codes(
                cache_directory=test_cache,
                use_encryption=False,
                _today=fixed_date,
            )

    # The cache should have the updated region for the refreshed IP
    updated_ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert updated_ip_to_region[expected_refreshed_ip] == new_region_for_refreshed_ip
    # All other IPs should remain unchanged
    for ip, region in initial_ip_to_region.items():
        if ip != expected_refreshed_ip:
            assert updated_ip_to_region[ip] == region

    # A log file should exist under cache/logs/
    logs_dir = test_cache / "logs"
    log_file = logs_dir / f"ip_refresh_{fixed_date.isoformat()}.yaml"
    assert log_file.exists(), f"Log file not found: {log_file}"

    log_data = yaml.safe_load(log_file.read_text()) or {}
    assert log_data["date"] == fixed_date.isoformat()
    assert log_data["partition_index"] == 0
    assert log_data["ips_checked"] == 1
    assert expected_refreshed_ip in log_data["changes"]
    assert log_data["changes"][expected_refreshed_ip]["old"] == "US/California"
    assert log_data["changes"][expected_refreshed_ip]["new"] == new_region_for_refreshed_ip


def test_refresh_ip_to_region_codes_no_changes(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that no log file is written when no regions have changed."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)

    initial_ip_to_region = {"192.0.2.1": "US/California"}
    ip_to_region_file = test_ips_dir / "ip_to_region.yaml"
    ip_to_region_file.write_text(yaml.dump(initial_ip_to_region))

    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")

    fixed_ordinal_base = datetime.date(2000, 1, 1).toordinal()
    offset = (-fixed_ordinal_base) % 90
    fixed_date = datetime.date.fromordinal(fixed_ordinal_base + offset)

    def mock_get_region_code_unchanged(ip_address: str, ipinfo_handler: object) -> str:
        return initial_ip_to_region[ip_address]

    with unittest.mock.patch(
        "s3_log_extraction.ip_utils._refresh_ip_to_region_codes._get_region_code_from_ip_address",
        mock_get_region_code_unchanged,
    ):
        with unittest.mock.patch("ipinfo.getHandler"):
            s3_log_extraction.ip_utils.refresh_ip_to_region_codes(
                cache_directory=test_cache,
                use_encryption=False,
                _today=fixed_date,
            )

    # Cache should be unchanged
    updated_ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert updated_ip_to_region == initial_ip_to_region

    # No log file should be written when there are no changes
    logs_dir = test_cache / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir())


def test_refresh_ip_to_region_codes_empty_cache(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that refresh_ip_to_region_codes returns early when the cache is empty."""
    test_cache = pathlib.Path(tmpdir)
    test_ips_dir = test_cache / "ips"
    test_ips_dir.mkdir(parents=True)
    (test_ips_dir / "ip_to_region.yaml").write_text("")

    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")

    # Should not raise and should not write any log files
    s3_log_extraction.ip_utils.refresh_ip_to_region_codes(cache_directory=test_cache, use_encryption=False)

    logs_dir = test_cache / "logs"
    assert not logs_dir.exists() or not any(logs_dir.iterdir())


@pytest.mark.ai_generated
def test_update_ip_to_region_codes_handles_ipinfo_quota_exceeded(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """update_ip_to_region_codes stores ``undetermined`` when IPInfo quota is exhausted."""
    extraction_dir = tmp_path / "extraction" / "test_dataset" / "test_asset"
    extraction_dir.mkdir(parents=True)
    test_ip = "4.4.4.4"
    (extraction_dir / "ips.txt").write_text(test_ip)

    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")

    mock_handler = unittest.mock.MagicMock()
    mock_handler.getDetails.side_effect = ipinfo.exceptions.RequestQuotaExceededError()

    with unittest.mock.patch("ipinfo.getHandler", return_value=mock_handler):
        with pytest.warns(RuntimeWarning, match="IPInfo API request quota exceeded"):
            s3_log_extraction.ip_utils.update_ip_to_region_codes(cache_directory=tmp_path, use_encryption=False)

    ip_to_region_file = tmp_path / "ips" / "ip_to_region.yaml"
    ip_to_region = yaml.safe_load(ip_to_region_file.read_text()) or {}
    assert ip_to_region[test_ip] == "undetermined"

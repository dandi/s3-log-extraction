import pathlib
import shutil

import py
import pytest

import s3_log_extraction


def test_ip_utils(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    test_cache = pathlib.Path(tmpdir)
    test_extraction_dir = test_cache / "extraction"
    test_ips_dir = test_cache / "ips"

    base_tests_dir = pathlib.Path(__file__).parent
    expected_cache = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_cache / "extraction"
    expected_ips_dir = expected_cache / "ips"

    # Provide non-None dummy keys so the guard checks in update functions pass without real credentials.
    # No actual API calls are made because the ips cache is pre-seeded with expected output below.
    monkeypatch.setenv("IPINFO_API_KEY", "test-key-non-remote")
    monkeypatch.setenv("OPENCAGE_API_KEY", "test-key-non-remote")

    # Emulate prior extraction by copying expected extraction directory and removing the expected indexed IP files
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)
    for file_path in test_extraction_dir.rglob(pattern="*indexed_ips*.txt"):
        file_path.unlink()

    # Test IP indexing
    s3_log_extraction.ip_utils.index_ips(cache_directory=test_cache, seed=0, encrypt=False)
    s3_log_extraction.testing.assert_filetree_matches(
        test_dir=test_extraction_dir, expected_dir=expected_extraction_dir
    )

    # Pre-seed the ips cache with expected output so the update functions see a complete cache and skip API calls.
    # With all IPs already present in index_to_region.yaml, indexes_to_update will be empty and no remote
    # calls are made.
    shutil.copytree(src=expected_ips_dir, dst=test_ips_dir, dirs_exist_ok=True)

    # Test updating indexed IPs to region codes and coordinates
    s3_log_extraction.ip_utils.update_index_to_region_codes(cache_directory=test_cache, encrypt=False)
    s3_log_extraction.ip_utils.update_region_code_coordinates(cache_directory=test_cache)
    s3_log_extraction.testing.assert_filetree_matches(test_dir=test_ips_dir, expected_dir=expected_ips_dir)

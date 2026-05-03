"""Tests for the inventory-based URL discovery in RemoteS3LogAccessExtractor."""

import io
import pathlib
import unittest.mock

import pytest

from s3_log_extraction.extractors._remote_s3_log_access_extractor import RemoteS3LogAccessExtractor


def _make_extractor(tmp_path: pathlib.Path) -> RemoteS3LogAccessExtractor:
    """
    Return an extractor backed by *tmp_path* with empty processing state.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Pytest-provided temporary directory used as the cache root.

    Returns
    -------
    RemoteS3LogAccessExtractor
        Extractor instance with ``processed_dates`` and
        ``s3_url_processing_end_record`` initialised to empty sets.
    """
    cache_directory = tmp_path / "cache"
    cache_directory.mkdir()
    extractor = RemoteS3LogAccessExtractor(cache_directory=cache_directory)
    extractor.processed_dates = set()
    extractor.s3_url_processing_end_record = set()
    return extractor


def _patch_fsspec_open(inventory_content: str) -> unittest.mock.MagicMock:
    """
    Build a ``MagicMock`` that can replace ``fsspec.open`` as a context manager.

    Parameters
    ----------
    inventory_content : str
        Text the mock file stream will return from ``.read()``.

    Returns
    -------
    unittest.mock.MagicMock
        Mock callable; its ``return_value`` is configured as a context manager
        that yields a ``StringIO`` wrapping *inventory_content*.
    """
    mock_open = unittest.mock.MagicMock()
    mock_open.return_value.__enter__.return_value = io.StringIO(inventory_content)
    mock_open.return_value.__exit__.return_value = False
    return mock_open


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_inventory_basic(tmp_path: pathlib.Path) -> None:
    """
    Inventory lines that match s3_root are parsed; all unprocessed URLs are
    returned when no dates have been processed yet.
    """
    extractor = _make_extractor(tmp_path)

    s3_root = "s3://my-bucket/logs"
    inventory_lines = [
        "s3://my-bucket/logs/2024/01/01/2024-01-01-00-00-00-AAAA",
        "s3://my-bucket/logs/2024/01/01/2024-01-01-00-05-00-BBBB",
        "s3://my-bucket/logs/2024/01/02/2024-01-02-00-00-00-CCCC",
    ]
    inventory_content = "\n".join(inventory_lines)

    with unittest.mock.patch("fsspec.open", _patch_fsspec_open(inventory_content)):
        result = extractor._get_unprocessed_s3_urls_from_inventory(
            inventory_s3_path="s3://my-bucket/inventory.txt",
            s3_root=s3_root,
        )

    assert set(result) == set(inventory_lines)


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_inventory_skips_processed_dates(tmp_path: pathlib.Path) -> None:
    """
    URLs whose dates are already in ``processed_dates`` are excluded.
    """
    extractor = _make_extractor(tmp_path)
    extractor.processed_dates = {"2024-01-01"}

    s3_root = "s3://my-bucket"
    inventory_content = "\n".join(
        [
            "s3://my-bucket/2024/01/01/2024-01-01-00-00-00-AAAA",
            "s3://my-bucket/2024/01/02/2024-01-02-00-00-00-BBBB",
        ]
    )

    with unittest.mock.patch("fsspec.open", _patch_fsspec_open(inventory_content)):
        result = extractor._get_unprocessed_s3_urls_from_inventory(
            inventory_s3_path="s3://my-bucket/inventory.txt",
            s3_root=s3_root,
        )

    assert result == ["s3://my-bucket/2024/01/02/2024-01-02-00-00-00-BBBB"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_inventory_skips_already_done_urls(tmp_path: pathlib.Path) -> None:
    """
    Individual URLs in ``s3_url_processing_end_record`` are excluded even when
    their date is otherwise unprocessed.
    """
    already_done = "s3://my-bucket/2024/01/01/2024-01-01-00-00-00-AAAA"
    extractor = _make_extractor(tmp_path)
    extractor.s3_url_processing_end_record = {already_done}

    s3_root = "s3://my-bucket"
    inventory_content = "\n".join(
        [
            already_done,
            "s3://my-bucket/2024/01/01/2024-01-01-00-05-00-BBBB",
        ]
    )

    with unittest.mock.patch("fsspec.open", _patch_fsspec_open(inventory_content)):
        result = extractor._get_unprocessed_s3_urls_from_inventory(
            inventory_s3_path="s3://my-bucket/inventory.txt",
            s3_root=s3_root,
        )

    assert result == ["s3://my-bucket/2024/01/01/2024-01-01-00-05-00-BBBB"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_inventory_ignores_non_matching_lines(tmp_path: pathlib.Path) -> None:
    """
    Lines that do not start with *s3_root* and blank lines are silently ignored.
    """
    extractor = _make_extractor(tmp_path)

    s3_root = "s3://my-bucket/logs"
    inventory_content = "\n".join(
        [
            "",
            "s3://other-bucket/2024/01/01/2024-01-01-00-00-00-AAAA",
            "s3://my-bucket/logs/2024/01/01/2024-01-01-00-00-00-BBBB",
            "   ",
        ]
    )

    with unittest.mock.patch("fsspec.open", _patch_fsspec_open(inventory_content)):
        result = extractor._get_unprocessed_s3_urls_from_inventory(
            inventory_s3_path="s3://my-bucket/logs/inventory.txt",
            s3_root=s3_root,
        )

    assert result == ["s3://my-bucket/logs/2024/01/01/2024-01-01-00-00-00-BBBB"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_from_inventory_ignores_short_paths(tmp_path: pathlib.Path) -> None:
    """
    Lines with fewer than four path components after the root are silently
    ignored (year/month/day/filename are all required).
    """
    extractor = _make_extractor(tmp_path)

    s3_root = "s3://my-bucket"
    inventory_content = "\n".join(
        [
            "s3://my-bucket/2024/01/",  # missing filename component (trailing slash)
            "s3://my-bucket/2024/01/01",  # exactly year/month/day with no filename
            "s3://my-bucket/2024/01/01/2024-01-01-00-00-00-VALID",
        ]
    )

    with unittest.mock.patch("fsspec.open", _patch_fsspec_open(inventory_content)):
        result = extractor._get_unprocessed_s3_urls_from_inventory(
            inventory_s3_path="s3://my-bucket/inventory.txt",
            s3_root=s3_root,
        )

    assert result == ["s3://my-bucket/2024/01/01/2024-01-01-00-00-00-VALID"]


@pytest.mark.ai_generated
def test_get_unprocessed_s3_urls_raises_when_both_manifest_and_inventory_provided(
    tmp_path: pathlib.Path,
) -> None:
    """
    Providing both ``manifest_file_path`` and ``inventory_s3_path`` simultaneously
    must raise a ``ValueError`` because they are mutually exclusive sources.
    """
    extractor = _make_extractor(tmp_path)

    # Create a minimal valid manifest file so pydantic doesn't complain
    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text("{}")

    with pytest.raises(ValueError, match="Only one of 'manifest_file_path' or 'inventory_s3_path'"):
        extractor._get_unprocessed_s3_urls(
            manifest_file_path=manifest_file,
            s3_root="s3://my-bucket",
            inventory_s3_path="s3://my-bucket/inventory.txt",
        )

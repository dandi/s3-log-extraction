"""Tests for RemoteS3LogAccessExtractor._update_records."""

import pathlib

import pytest
import yaml

from s3_log_extraction.extractors._remote_s3_log_access_extractor import RemoteS3LogAccessExtractor


def _make_extractor(tmp_path: pathlib.Path) -> RemoteS3LogAccessExtractor:
    """Create an extractor instance pointing at a temporary cache directory."""
    extractor = RemoteS3LogAccessExtractor.__new__(RemoteS3LogAccessExtractor)
    extractor.cache_directory = tmp_path
    extractor.records_directory = tmp_path / "records"
    extractor.records_directory.mkdir(parents=True, exist_ok=True)

    class_name = RemoteS3LogAccessExtractor.__name__
    extractor.s3_url_processing_end_record_file_path = (
        extractor.records_directory / f"{class_name}_s3-url-processing-end.txt"
    )
    extractor.processed_years_record_file_path = extractor.records_directory / "processed_years.yaml"
    extractor.processed_months_per_year_record_file_path = (
        extractor.records_directory / "processed_months_per_year.yaml"
    )

    extractor.processed_years: set[str] = set()
    extractor.processed_months_per_year: dict[str, set[str]] = {}
    extractor.processed_dates: set[str] = set()
    extractor._s3_urls_per_date_manifest: dict[str, list[str]] = {}
    extractor._s3_urls_per_date_remote: dict[str, list[str]] = {}
    extractor.unprocessed_months_per_year: dict[str, list[str]] = {}

    return extractor


@pytest.mark.ai_generated
def test_update_records_no_end_record(tmp_path: pathlib.Path) -> None:
    """_update_records returns early when the end-record file does not exist."""
    extractor = _make_extractor(tmp_path)

    # No end-record file written → method must not raise and must not create YAML files
    extractor._update_records()

    assert not (extractor.records_directory / "processed_dates.yaml").exists()
    assert not extractor.processed_years_record_file_path.exists()
    assert not extractor.processed_months_per_year_record_file_path.exists()


@pytest.mark.ai_generated
def test_update_records_marks_date_done(tmp_path: pathlib.Path) -> None:
    """A date whose every URL is in the end-record is added to processed_dates.yaml."""
    extractor = _make_extractor(tmp_path)

    s3_url_a = "s3://bucket/2024/01/15/log-a.txt"
    s3_url_b = "s3://bucket/2024/01/15/log-b.txt"
    extractor.s3_url_processing_end_record_file_path.write_text(f"{s3_url_a}\n{s3_url_b}\n")
    extractor._s3_urls_per_date_remote = {"2024-01-15": [s3_url_a, s3_url_b]}

    extractor._update_records()

    assert "2024-01-15" in extractor.processed_dates
    with (extractor.records_directory / "processed_dates.yaml").open() as file_stream:
        loaded = yaml.safe_load(file_stream)
    assert "2024-01-15" in loaded


@pytest.mark.ai_generated
def test_update_records_partial_date_not_marked(tmp_path: pathlib.Path) -> None:
    """A date with only some URLs processed is NOT added to processed_dates."""
    extractor = _make_extractor(tmp_path)

    s3_url_a = "s3://bucket/2024/01/15/log-a.txt"
    s3_url_b = "s3://bucket/2024/01/15/log-b.txt"
    # Only url_a in end record
    extractor.s3_url_processing_end_record_file_path.write_text(f"{s3_url_a}\n")
    extractor._s3_urls_per_date_remote = {"2024-01-15": [s3_url_a, s3_url_b]}

    extractor._update_records()

    assert "2024-01-15" not in extractor.processed_dates


@pytest.mark.ai_generated
def test_update_records_full_month_promotes_to_processed_months(tmp_path: pathlib.Path) -> None:
    """When all calendar days in a month are in processed_dates, the month is marked done."""
    import calendar

    extractor = _make_extractor(tmp_path)
    year, month = "2023", "02"

    # Build a complete set of dates for February 2023 (28 days)
    total_days = calendar.monthrange(int(year), int(month))[1]
    dates = [f"{year}-{month}-{day:02d}" for day in range(1, total_days + 1)]

    # Simulate all dates already in processed_dates (from a previous run)
    extractor.processed_dates = set(dates)

    # Seed the end-record with a dummy URL so _update_records doesn't return early
    dummy_url = f"s3://bucket/{year}/{month}/01/log.txt"
    extractor.s3_url_processing_end_record_file_path.write_text(f"{dummy_url}\n")

    # The remote scan found this month as unprocessed
    extractor.unprocessed_months_per_year = {year: [month]}

    extractor._update_records()

    assert month in extractor.processed_months_per_year.get(year, set())


@pytest.mark.ai_generated
def test_update_records_full_year_promotes_to_processed_years(tmp_path: pathlib.Path) -> None:
    """When all 12 months in a year are processed, the year is added to processed_years."""
    import calendar

    extractor = _make_extractor(tmp_path)
    year = "2022"

    # Populate processed_months_per_year with all 12 months pre-marked (simulating prior runs)
    extractor.processed_months_per_year[year] = {f"{m:02d}" for m in range(1, 13)}

    # Build a full set of dates for every month to satisfy the calendar-day check
    all_dates: set[str] = set()
    for m in range(1, 13):
        month_str = f"{m:02d}"
        total_days = calendar.monthrange(int(year), m)[1]
        for day in range(1, total_days + 1):
            all_dates.add(f"{year}-{month_str}-{day:02d}")
    extractor.processed_dates = all_dates

    dummy_url = f"s3://bucket/{year}/01/01/log.txt"
    extractor.s3_url_processing_end_record_file_path.write_text(f"{dummy_url}\n")
    extractor.unprocessed_months_per_year = {year: [f"{m:02d}" for m in range(1, 13)]}

    extractor._update_records()

    assert year in extractor.processed_years


@pytest.mark.ai_generated
def test_update_records_yaml_files_written(tmp_path: pathlib.Path) -> None:
    """_update_records writes all three YAML record files."""
    extractor = _make_extractor(tmp_path)

    dummy_url = "s3://bucket/2024/03/10/log.txt"
    extractor.s3_url_processing_end_record_file_path.write_text(f"{dummy_url}\n")
    extractor._s3_urls_per_date_remote = {"2024-03-10": [dummy_url]}

    extractor._update_records()

    assert (extractor.records_directory / "processed_dates.yaml").exists()
    assert extractor.processed_years_record_file_path.exists()
    assert extractor.processed_months_per_year_record_file_path.exists()

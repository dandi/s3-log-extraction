import json
import pathlib
import shutil

import pandas
import py
import pytest

import s3_log_extraction


def test_generic_summaries(tmpdir: py.path.local):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir, use_encryption=False)
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    test_file_paths = {path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="*.tsv")}
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path for path in expected_summaries_dir.rglob(pattern="*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)

    # Verify requester_count.tsv files
    test_tsv_paths = {
        path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="requester_count.tsv")
    }
    expected_tsv_paths = {
        path.relative_to(expected_summaries_dir): path
        for path in expected_summaries_dir.rglob(pattern="requester_count.tsv")
    }
    assert set(test_tsv_paths.keys()) == set(expected_tsv_paths.keys())

    for relative_path, expected_txt_path in expected_tsv_paths.items():
        test_txt_path = test_summary_dir / relative_path
        assert test_txt_path.read_text().strip() == expected_txt_path.read_text().strip(), (
            f"\n\nMismatch in {relative_path}:\n"
            f"  test:     {test_txt_path.read_text().strip()!r}\n"
            f"  expected: {expected_txt_path.read_text().strip()!r}\n"
        )

    # Verify totals.json
    test_totals = json.loads((test_summary_dir / "totals.json").read_text())
    expected_totals = json.loads((expected_summaries_dir / "totals.json").read_text())
    assert (
        test_totals == expected_totals
    ), f"\n\ntotals.json mismatch:\n  test:     {test_totals}\n  expected: {expected_totals}\n"

    # Verify archive_totals.json
    test_archive_totals = json.loads((test_summary_dir / "archive_totals.json").read_text())
    expected_archive_totals = json.loads((expected_summaries_dir / "archive_totals.json").read_text())
    assert (
        test_archive_totals == expected_archive_totals
    ), f"\n\narchive_totals.json mismatch:\n  test:     {test_archive_totals}\n  expected: {expected_archive_totals}\n"


def test_generate_all_dataset_totals_skips_archive(tmpdir: py.path.local):
    """Verify that the 'archive' subdirectory is excluded from dataset totals."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    # Set up a real dataset summary
    dataset_dir = summary_dir / "ds001161"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t1194564\t4\t3\n"
    )

    # Set up an archive summary that should be excluded
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t7481053\t7\t5\n"
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert "ds001161" in totals, "'ds001161' should be present in totals.json"
    assert "archive" not in totals, "'archive' should be excluded from totals.json"


@pytest.mark.ai_generated
def test_generate_archive_totals_raises_without_archive_requester_count(tmpdir: py.path.local) -> None:
    """Archive totals should fail if archive requester count has not been generated."""
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t7481053\t7\t5\n"
    )

    with pytest.raises(FileNotFoundError, match="Archive requester count file not found"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "downloads", "minimum_threshold", "expected_requests", "expected_downloads"),
    [
        (4, 14, 5, "<5", 20),
        (33, 36, 5, 40, 40),
    ],
)
def test_generate_archive_totals_thresholds_request_and_download_counts(
    tmpdir: py.path.local,
    requests: int,
    downloads: int,
    minimum_threshold: int,
    expected_requests: str | int,
    expected_downloads: str | int,
) -> None:
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" f"missing\t10\t{requests}\t{downloads}\n"
    )
    (archive_dir / "requester_count.tsv").write_text("100\n")

    s3_log_extraction.summarize.generate_archive_totals(
        cache_directory=test_dir, privacy_threshold_minimum=minimum_threshold
    )

    archive_totals = json.loads((test_dir / "summaries" / "archive_totals.json").read_text())
    assert archive_totals["total_number_of_requests"] == expected_requests
    assert archive_totals["total_number_of_downloads"] == expected_downloads


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "downloads", "minimum_threshold", "expected_requests", "expected_downloads"),
    [
        (4, 14, 5, "<5", 20),
        (33, 36, 5, 40, 40),
    ],
)
def test_generate_all_dataset_totals_thresholds_request_and_download_counts(
    tmpdir: py.path.local,
    requests: int,
    downloads: int,
    minimum_threshold: int,
    expected_requests: str | int,
    expected_downloads: str | int,
) -> None:
    test_dir = pathlib.Path(tmpdir)
    dataset_dir = test_dir / "summaries" / "ds001"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" f"missing\t10\t{requests}\t{downloads}\n"
    )
    (dataset_dir / "requester_count.tsv").write_text("100\n")

    s3_log_extraction.summarize.generate_all_dataset_totals(
        cache_directory=test_dir, privacy_threshold_minimum=minimum_threshold
    )

    totals = json.loads((test_dir / "summaries" / "totals.json").read_text())
    assert totals["ds001"]["total_number_of_requests"] == expected_requests
    assert totals["ds001"]["total_number_of_downloads"] == expected_downloads


@pytest.mark.ai_generated
def test_generate_archive_summaries_thresholds_request_and_download_columns(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir, privacy_threshold_minimum=4)

    archive_by_day = pandas.read_table(filepath_or_buffer=summary_dir / "archive" / "by_day.tsv")
    archive_by_region = pandas.read_table(filepath_or_buffer=summary_dir / "archive" / "by_region.tsv")
    assert archive_by_day.loc[0, "number_of_requests"] == "<4"
    assert archive_by_day.loc[0, "number_of_downloads"] == "<4"
    assert archive_by_region.loc[0, "number_of_requests"] == "<4"
    assert archive_by_region.loc[0, "number_of_downloads"] == "<4"


@pytest.mark.ai_generated
def test_generate_archive_summaries_aggregates_requester_count(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_requester_count_file_path = summary_dir / "archive" / "requester_count.tsv"
    assert archive_requester_count_file_path.exists()
    assert archive_requester_count_file_path.read_text().strip() == "100"


@pytest.mark.ai_generated
def test_generate_archive_summaries_aggregates_optional_by_asset_type_per_week(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n2026-01-05\t3\t4\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n2026-01-05\t7\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    assert archive_file_path.exists()
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    expected_summary = pandas.DataFrame(
        data={
            "week_start": ["2025-12-29", "2026-01-05"],
            "Miscellaneous": [2, 4],
            "Neurophysiology": [1, 3],
            "Video": [5, 7],
        }
    )
    pandas.testing.assert_frame_equal(left=archive_summary, right=expected_summary)


@pytest.mark.ai_generated
def test_generate_archive_summaries_accepts_custom_asset_type_order(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n")

    s3_log_extraction.summarize.generate_archive_summaries(
        cache_directory=test_dir, asset_types_in_order=["Video", "Neurophysiology", "Miscellaneous"]
    )

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    assert archive_summary.columns.tolist() == ["week_start", "Video", "Neurophysiology", "Miscellaneous"]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("count", "modulo", "minimum", "expected"),
    [
        # Below minimum → sentinel string
        (0, 20, 50, "<50"),
        (1, 20, 50, "<50"),
        (49, 20, 50, "<50"),
        # At or above minimum → rounded to nearest multiple of modulo
        (50, 20, 50, 40),  # round(2.5)=2 (banker's rounding)
        (55, 20, 50, 60),  # round(2.75)=3
        (60, 20, 50, 60),
        (100, 20, 50, 100),
        (123, 20, 50, 120),
        # Custom modulo and minimum
        (4, 5, 5, "<5"),
        (5, 5, 5, 5),
        (7, 5, 5, 5),
        (8, 5, 5, 10),
        # minimum can differ from modulo
        (9, 10, 5, 10),
        (3, 10, 5, "<5"),
    ],
)
def test_round_requester_count(count: int, modulo: int, minimum: int, expected: str | int):
    """Privacy-rounding returns the sentinel below minimum and rounds to the nearest modulo otherwise."""
    from s3_log_extraction.summarize._generate_summaries import _round_requester_count

    assert _round_requester_count(count=count, modulo=modulo, minimum=minimum) == expected

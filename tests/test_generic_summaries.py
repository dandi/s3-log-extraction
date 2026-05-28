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
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" "missing\t1194564\t4\t3\n"
    )

    # Set up an archive summary that should be excluded
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" "missing\t7481053\t7\t5\n"
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
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" "missing\t7481053\t7\t5\n"
    )

    with pytest.raises(FileNotFoundError, match="Archive requester count file not found"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)


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

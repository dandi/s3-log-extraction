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

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_all_dataset_totals(summary_directory=test_summary_dir)
    s3_log_extraction.summarize.generate_archive_summaries(summary_directory=test_summary_dir)
    s3_log_extraction.summarize.generate_archive_totals(summary_directory=test_summary_dir)

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

    # Verify requester_count.txt files
    test_txt_paths = {
        path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="requester_count.txt")
    }
    expected_txt_paths = {
        path.relative_to(expected_summaries_dir): path
        for path in expected_summaries_dir.rglob(pattern="requester_count.txt")
    }
    assert set(test_txt_paths.keys()) == set(expected_txt_paths.keys())

    for relative_path, expected_txt_path in expected_txt_paths.items():
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


@pytest.mark.ai_generated
def test_round_requester_count_below_minimum():
    """Counts strictly below ``minimum`` produce the ``"<{minimum}"`` sentinel."""
    from s3_log_extraction.summarize._generate_summaries import _round_requester_count

    assert _round_requester_count(count=0, modulo=20, minimum=50) == "<50"
    assert _round_requester_count(count=1, modulo=20, minimum=50) == "<50"
    assert _round_requester_count(count=49, modulo=20, minimum=50) == "<50"


@pytest.mark.ai_generated
def test_round_requester_count_at_and_above_minimum():
    """Counts at or above ``minimum`` are rounded to the nearest multiple of ``modulo``."""
    from s3_log_extraction.summarize._generate_summaries import _round_requester_count

    assert _round_requester_count(count=50, modulo=20, minimum=50) == 40  # round(2.5)=2 (banker's rounding)
    assert _round_requester_count(count=55, modulo=20, minimum=50) == 60  # round(2.75)=3
    assert _round_requester_count(count=60, modulo=20, minimum=50) == 60
    assert _round_requester_count(count=100, modulo=20, minimum=50) == 100
    assert _round_requester_count(count=123, modulo=20, minimum=50) == 120


@pytest.mark.ai_generated
def test_round_requester_count_custom_modulo_and_minimum():
    """Custom ``modulo`` and ``minimum`` values are each respected independently."""
    from s3_log_extraction.summarize._generate_summaries import _round_requester_count

    assert _round_requester_count(count=4, modulo=5, minimum=5) == "<5"
    assert _round_requester_count(count=5, modulo=5, minimum=5) == 5
    assert _round_requester_count(count=7, modulo=5, minimum=5) == 5
    assert _round_requester_count(count=8, modulo=5, minimum=5) == 10
    # minimum can differ from modulo
    assert _round_requester_count(count=9, modulo=10, minimum=5) == 10
    assert _round_requester_count(count=3, modulo=10, minimum=5) == "<5"

"""CLI integration tests that mirror the existing API-based integration tests."""

import os
import pathlib
import shutil
import subprocess

import pandas
import py

import s3_log_extraction


def _run_cli_extraction_test(tmpdir: py.path.local, workers: int) -> None:
    """
    Helper function to run CLI extraction tests with a specified number of workers.

    Parameters
    ----------
    tmpdir : py.path.local
        Temporary directory for test outputs.
    workers : int
        Number of workers to use for extraction.
    """
    tmpdir = pathlib.Path(tmpdir)

    base_directory = pathlib.Path(__file__).parent
    test_logs_directory = base_directory / "example_logs"
    output_directory = tmpdir / "test_extraction"
    output_directory.mkdir(exist_ok=True)
    expected_output_directory = base_directory / "expected_output"

    # Run extraction via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "config",
            "cache",
            "set",
            str(output_directory),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    result = subprocess.run(
        [
            "s3logextraction",
            "extract",
            str(test_logs_directory),
            "--workers",
            str(workers),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0

    # Verify output files match expected structure
    relative_output_files = {file.relative_to(output_directory) for file in output_directory.rglob(pattern="*.txt")}
    relative_expected_files = {
        file.relative_to(expected_output_directory) for file in expected_output_directory.rglob(pattern="*.txt")
    }
    assert relative_output_files == relative_expected_files

    # Verify content matches expected output
    s3_log_extraction.testing.assert_expected_extraction_content(
        extractor_name="S3LogAccessExtractor",
        test_directory=base_directory,
        output_directory=output_directory,
        expected_output_directory=expected_output_directory,
        relative_output_files=relative_output_files,
        relative_expected_files=relative_expected_files,
    )


def test_cli_extraction(tmpdir: py.path.local) -> None:
    """Test extraction using the CLI instead of the API."""
    _run_cli_extraction_test(tmpdir, workers=1)


def test_cli_extraction_parallel(tmpdir: py.path.local) -> None:
    """Test parallel extraction using the CLI instead of the API."""
    _run_cli_extraction_test(tmpdir, workers=2)


def test_cli_generic_summaries(tmpdir: py.path.local) -> None:
    """Test summary generation using the CLI instead of the API."""
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)

    # Create environment with password for encryption
    env = os.environ.copy()
    env["S3_LOG_EXTRACTION_PASSWORD"] = "test"

    # Set cache directory via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "config",
            "cache",
            "set",
            str(test_dir),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Update IP indexes via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "update",
            "ip",
            "indexes",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Generate summaries via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "update",
            "summaries",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Generate dataset totals via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "update",
            "totals",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Generate archive summaries via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "update",
            "summaries",
            "--mode",
            "archive",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Generate archive totals via CLI
    result = subprocess.run(
        [
            "s3logextraction",
            "update",
            "totals",
            "--mode",
            "archive",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0

    # Verify the output matches expected files
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

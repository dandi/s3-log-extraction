import json
import pathlib

import pandas
import py

import s3_log_extraction


def test_map_all_reduced_s3_logs_to_dandisets(tmpdir: py.path.local):
    tmpdir = pathlib.Path(tmpdir)

    file_parent = pathlib.Path(__file__).parent
    examples_folder_path = file_parent / "examples" / "mapped_to_dandisets_example_0"
    example_binned_s3_logs_folder_path = examples_folder_path / "binned_logs"

    test_mapped_s3_logs_folder_path = tmpdir

    expected_output_folder_path = examples_folder_path / "expected_output"

    s3_log_extraction.map_binned_s3_logs_to_dandisets(
        binned_s3_logs_folder_path=example_binned_s3_logs_folder_path,
        mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path,
    )

    s3_log_extraction.generate_archive_summaries(mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path)

    test_file_paths = {
        path.relative_to(test_mapped_s3_logs_folder_path): path
        for path in test_mapped_s3_logs_folder_path.rglob("*.tsv")
    }
    expected_file_paths = {
        path.relative_to(expected_output_folder_path): path for path in expected_output_folder_path.rglob("*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_output_folder_path)
        test_file_path = test_mapped_s3_logs_folder_path / relative_file_path

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

    # TODO: make a standalone test case (requires setting up expected test output example outside live services)
    s3_log_extraction.generate_all_dandiset_totals(mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path)

    test_all_dandiset_totals_file_path = test_mapped_s3_logs_folder_path / "all_dandiset_totals.json"
    expected_all_dandiset_totals_file_path = expected_output_folder_path / "all_dandiset_totals.json"
    with test_all_dandiset_totals_file_path.open(mode="r") as io:
        test_all_dandiset_totals = json.load(fp=io)
    with expected_all_dandiset_totals_file_path.open(mode="r") as io:
        expected_all_dandiset_totals = json.load(fp=io)

    assert test_all_dandiset_totals == expected_all_dandiset_totals

    # TODO: make a standalone test case (requires setting up expected test output example outside live services)
    s3_log_extraction.generate_archive_totals(mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path)

    test_archive_totals_file_path = test_mapped_s3_logs_folder_path / "archive_totals.json"
    expected_all_dandiset_totals_file_path = expected_output_folder_path / "archive_totals.json"
    with test_archive_totals_file_path.open(mode="r") as io:
        test_archive_totals = json.load(fp=io)
    with expected_all_dandiset_totals_file_path.open(mode="r") as io:
        expected_archive_totals = json.load(fp=io)

    assert test_archive_totals == expected_archive_totals

    # TODO: make a standalone test case (requires setting up expected test output example outside live services)
    test_cache_directory = test_mapped_s3_logs_folder_path / ".cache"
    test_cache_directory.mkdir(exist_ok=True)
    s3_log_extraction.update_region_codes_to_coordinates(
        mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path, cache_directory=test_cache_directory
    )

    test_log_cache_directory = test_cache_directory / "s3_log_extraction"
    test_region_codes_to_coordinates_file_path = test_log_cache_directory / "region_codes_to_coordinates.json"
    expected_log_cache_directory = expected_output_folder_path / ".cache" / "s3_log_extraction"
    expected_region_codes_to_coordinates_file_path = expected_log_cache_directory / "region_codes_to_coordinates.json"
    with test_region_codes_to_coordinates_file_path.open(mode="r") as io:
        test_region_codes_to_coordinates = json.load(fp=io)
    with expected_region_codes_to_coordinates_file_path.open(mode="r") as io:
        expected_region_codes_to_coordinates = json.load(fp=io)

    assert test_region_codes_to_coordinates == expected_region_codes_to_coordinates

import pathlib
import shutil

import pandas
import py

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
    s3_log_extraction.ip_utils.index_ips(cache_directory=test_dir)

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

    # # TODO: make a standalone test case (requires setting up expected test output example outside live services)
    # s3_log_extraction.generate_archive_totals(mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path)
    #
    # test_archive_totals_file_path = test_mapped_s3_logs_folder_path / "archive_totals.json"
    # expected_all_dandiset_totals_file_path = expected_output_folder_path / "archive_totals.json"
    # with test_archive_totals_file_path.open(mode="r") as io:
    #     test_archive_totals = json.load(fp=io)
    # with expected_all_dandiset_totals_file_path.open(mode="r") as io:
    #     expected_archive_totals = json.load(fp=io)
    #
    # assert test_archive_totals == expected_archive_totals
    #
    # # TODO: make a standalone test case (requires setting up expected test output example outside live services)
    # test_cache_directory = test_mapped_s3_logs_folder_path / ".cache"
    # test_cache_directory.mkdir(exist_ok=True)
    # s3_log_extraction.update_region_codes_to_coordinates(
    #     mapped_s3_logs_folder_path=test_mapped_s3_logs_folder_path, cache_directory=test_cache_directory
    # )
    #
    # test_log_cache_directory = test_cache_directory / "dandi_s3_log_extraction"
    # test_region_codes_to_coordinates_file_path = test_log_cache_directory / "region_codes_to_coordinates.json"
    # expected_log_cache_directory = expected_output_folder_path / ".cache" / "dandi_s3_log_extraction"
    # expected_region_codes_to_coordinates_file_path = expected_log_cache_directory / "region_codes_to_coordinates.json"
    # with test_region_codes_to_coordinates_file_path.open(mode="r") as io:
    #     test_region_codes_to_coordinates = json.load(fp=io)
    # with expected_region_codes_to_coordinates_file_path.open(mode="r") as io:
    #     expected_region_codes_to_coordinates = json.load(fp=io)
    #
    # assert test_region_codes_to_coordinates == expected_region_codes_to_coordinates

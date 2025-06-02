import pathlib

import py

import s3_log_extraction


def test_extraction(tmpdir: py.path.local) -> None:
    tmpdir = pathlib.Path(tmpdir)

    base_directory = pathlib.Path(__file__).parent
    test_logs_directory = base_directory / "test_logs"
    output_directory = tmpdir / "test_extraction"
    output_directory.mkdir(exist_ok=True)
    expected_output_directory = base_directory / "expected_output"

    extractor = s3_log_extraction.extract.S3LogAccessExtractor(cache_directory=output_directory)
    extractor.extract_directory(directory=test_logs_directory)

    relative_output_files = set(
        [file.relative_to(output_directory) for file in output_directory.rglob(pattern="*.txt")]
    )
    relative_expected_files = set(
        [file.relative_to(expected_output_directory) for file in expected_output_directory.rglob(pattern="*.txt")]
    )
    assert relative_output_files == relative_expected_files

    record_files = {
        pathlib.Path("records/S3LogAccessExtractor_extraction.txt"),
        pathlib.Path("records/S3LogAccessExtractor_mirror-copy-end.txt"),
        pathlib.Path("records/S3LogAccessExtractor_mirror-copy-start.txt"),
    }
    non_record_output_files = relative_output_files - record_files
    non_record_expected_files = relative_expected_files - record_files
    for relative_output_file, relative_expected_file in zip(non_record_output_files, non_record_expected_files):
        output_file = output_directory / relative_output_file
        expected_file = expected_output_directory / relative_expected_file
        with output_file.open(mode="rb") as file_stream_1, expected_file.open(mode="rb") as file_stream_2:
            output_content = file_stream_1.read()
            expected_content = file_stream_2.read()
            assert output_content == expected_content

    relative_testing_directory = pathlib.Path(__file__).parent
    for record_file in record_files:
        output_file = output_directory / record_file
        expected_file = expected_output_directory / record_file
        with output_file.open(mode="r") as file_stream_1, expected_file.open(mode="r") as file_stream_2:
            output_content = set(
                [
                    relative_testing_directory / pathlib.Path(file_path).name
                    for file_path in file_stream_1.read().splitlines()
                ]
            )
            expected_content = set(
                [
                    relative_testing_directory / pathlib.Path(file_path).name
                    for file_path in file_stream_2.read().splitlines()
                ]
            )
            assert output_content == expected_content


# TODO: parallel case, CLI

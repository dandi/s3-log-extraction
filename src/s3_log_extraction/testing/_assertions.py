import pathlib


def assert_expected_extraction_content(
    extractor_name: str,
    output_directory: pathlib.Path,
    expected_output_directory: pathlib.Path,
    relative_output_files: pathlib.Path,
    relative_expected_files: pathlib.Path,
) -> None:
    """Check if the expected content and records match the actual content and records."""
    record_files = {
        pathlib.Path(f"records/{extractor_name}_extraction.txt"),
        pathlib.Path(f"records/{extractor_name}_mirror-copy-end.txt"),
        pathlib.Path(f"records/{extractor_name}_mirror-copy-start.txt"),
    }
    non_record_output_files = relative_output_files - record_files
    non_record_expected_files = relative_expected_files - record_files
    for relative_output_file, relative_expected_file in zip(non_record_output_files, non_record_expected_files):
        output_file = output_directory / relative_output_file
        expected_file = expected_output_directory / relative_expected_file
        with output_file.open(mode="rb") as file_stream_1, expected_file.open(mode="rb") as file_stream_2:
            output_content = file_stream_1.read()
            expected_content = file_stream_2.read()
            assert output_content == expected_content, f"Binary content mismatch in {relative_output_file}"
    for record_file in record_files:
        output_file = output_directory / record_file
        expected_file = expected_output_directory / record_file
        with output_file.open(mode="r") as file_stream_1, expected_file.open(mode="r") as file_stream_2:
            output_content = set(file_stream_1.read().splitlines())
            expected_content = set(file_stream_2.read().splitlines())
            assert output_content == expected_content, (
                f"Line set mismatch in {record_file}.\n"
                f"Extra in output: {output_content - expected_content}\n"
                f"Missing in output: {expected_content - output_content}"
            )

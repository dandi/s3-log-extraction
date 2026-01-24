import pathlib


def assert_expected_extraction_content(
    extractor_name: str,
    test_directory: pathlib.Path,
    output_directory: pathlib.Path,
    expected_output_directory: pathlib.Path,
    relative_output_files: pathlib.Path,
    relative_expected_files: pathlib.Path,
) -> None:
    """Check if the expected content and records match the actual content and records."""
    record_files = {
        pathlib.Path(f"records/{extractor_name}_file-processing-end.txt"),
        pathlib.Path(f"records/{extractor_name}_file-processing-start.txt"),
    }
    non_record_output_files = sorted(relative_output_files - record_files)
    non_record_expected_files = sorted(relative_expected_files - record_files)

    for relative_output_file, relative_expected_file in zip(non_record_output_files, non_record_expected_files):
        output_file = output_directory / relative_output_file
        expected_file = expected_output_directory / relative_expected_file
        with output_file.open(mode="rb") as file_stream_1, expected_file.open(mode="rb") as file_stream_2:
            output_content = file_stream_1.read()
            expected_content = file_stream_2.read().replace(b"\r\n", b"\n")  # Bug fix for WSL
            message = (
                f"Binary content mismatch between:\n\n"
                f"\n{output_file=}\n"
                f"{relative_output_file=}\n"
                f"{expected_file=}\n\n"
                f"{output_content=}\n"
                f"{expected_content=}\n\n"
            )
            assert output_content == expected_content, message
    for record_file in record_files:
        output_file = output_directory / record_file
        expected_file = expected_output_directory / record_file

        test_directory = str(test_directory)
        expected_test_directory = r"E:\GitHub\s3-log-extraction\tests\extraction"

        with output_file.open(mode="r") as file_stream_1, expected_file.open(mode="r") as file_stream_2:
            output_content = set(line.removeprefix(test_directory) for line in file_stream_1.read().splitlines())
            expected_content = set(
                line.removeprefix(expected_test_directory).replace("\\", "/")
                for line in file_stream_2.read().splitlines()
            )

            assert output_content == expected_content, (
                f"Line set mismatch in {record_file}.\n"
                f"Extra in output: {output_content - expected_content}\n"
                f"Missing in output: {expected_content - output_content}"
            )


def assert_filetree_matches(test_dir: pathlib.Path, expected_dir: pathlib.Path) -> None:
    relative_test_file_contents = {
        file.relative_to(test_dir): file.read_bytes().strip() for file in test_dir.rglob(pattern="*") if file.is_file()
    }
    relative_expected_file_contents = {
        file.relative_to(expected_dir): file.read_bytes().strip()
        for file in expected_dir.rglob(pattern="*")
        if file.is_file()
    }

    test_files = set(relative_test_file_contents.keys())
    expected_files = set(relative_expected_file_contents.keys())
    assert test_files == expected_files, (
        f"File trees do not match.\n"
        f"Test directory: {test_dir}\n"
        f"Expected directory: {expected_dir}\n"
        f"Extra files in test: {test_files - expected_files}\n"
        f"Missing files in test: {expected_files - test_files}"
    )

    for relative_file_path in relative_expected_file_contents.keys():
        test_content = relative_test_file_contents[relative_file_path]
        expected_content = relative_expected_file_contents[relative_file_path]

        assert test_content == expected_content, (
            f"\n\nContent mismatch in file: {relative_file_path}\n"
            f"Test file: {test_dir / relative_file_path}\n"
            f"Expected file: {expected_dir / relative_file_path}\n"
            f"Test content length: {len(test_content)} bytes\n"
            f"Expected content length: {len(expected_content)} bytes\n"
            f"Test content: {test_content}\n"
            f"Expected content: {expected_content}\n\n"
        )

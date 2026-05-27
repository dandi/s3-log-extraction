import pathlib


def test_get_extraction_directory_removed_from_config_api() -> None:
    config_init_path = pathlib.Path(__file__).parent.parent / "src" / "s3_log_extraction" / "config" / "__init__.py"
    config_init_content = config_init_path.read_text()
    assert "get_extraction_directory" not in config_init_content

"""CLI integration tests for the validate, reset, stop, and config commands."""

import pathlib

import pytest
from click.testing import CliRunner

import s3_log_extraction

_EXAMPLE_LOGS_DIRECTORY = pathlib.Path(__file__).parent / "example_logs"
_VALIDATION_PROTOCOLS = (
    "downloads_logic",
    "http_empty_split",
    "http_split_count",
    "extraction_heuristic",
    "timestamps_parsing",
)
# A well-formed `REST.GET.OBJECT` line: a complete 200 download by a documentation-range requester
_CLEAN_LOG_LINE = (
    "abc123 dandiarchive [01/Jan/2020:05:06:35 +0000] 192.0.2.0 - J42N2W7ET0EC03CV REST.GET.OBJECT "
    'ds006260/sub-Re19/file.tsv "GET /ds006260/sub-Re19/file.tsv HTTP/1.1" 200 - 384 384 53 52 "-" "-" - '
    "- - ECDHE-RSA-AES128-GCM-SHA256 - dandiarchive.s3.amazonaws.com TLSv1.2 -\n"
)


@pytest.fixture
def runner() -> CliRunner:
    """A click runner for invoking the `s3logextraction` command line interface."""
    return CliRunner()


@pytest.fixture
def isolated_records_directory(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> pathlib.Path:
    """Point the validator record cache at a temporary directory rather than the user's real cache."""
    records_directory = tmp_path / "validator_records"
    records_directory.mkdir()
    monkeypatch.setattr(
        "s3_log_extraction.validate._base_validator.get_cache_subdirectory", lambda **kwargs: records_directory
    )
    return records_directory


@pytest.fixture
def clean_logs_directory(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    A directory of well-formed `.log` files, which is what the validators traverse a directory for.

    The bundled example logs deliberately contain aberrant lines for the extraction tests, so they are not
    usable here.
    """
    log_directory = tmp_path / "logs"
    nested_directory = log_directory / "nested"
    nested_directory.mkdir(parents=True)
    for index, log_directory_of_file in enumerate((log_directory, log_directory, nested_directory)):
        (log_directory_of_file / f"{index}.log").write_text(_CLEAN_LOG_LINE)
    return log_directory


@pytest.mark.ai_generated
@pytest.mark.parametrize("protocol", _VALIDATION_PROTOCOLS)
def test_cli_validate(
    runner: CliRunner,
    isolated_records_directory: pathlib.Path,
    clean_logs_directory: pathlib.Path,
    protocol: str,
) -> None:
    """Every validation protocol should pass over well-formed logs and record what it validated."""
    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["validate", protocol, str(clean_logs_directory)])

    assert result.exit_code == 0, f"Validation failed: {result.output}"

    recorded_lines = {
        line for record_file in isolated_records_directory.iterdir() for line in record_file.read_text().splitlines()
    }

    assert recorded_lines == {str(log_file.absolute()) for log_file in clean_logs_directory.rglob(pattern="*.log")}


@pytest.mark.ai_generated
def test_cli_validate_rejects_unknown_protocol(runner: CliRunner) -> None:
    """An unrecognized protocol should be rejected by the command line interface itself."""
    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli, ["validate", "not_a_protocol", str(_EXAMPLE_LOGS_DIRECTORY)]
    )

    assert result.exit_code != 0


@pytest.mark.ai_generated
def test_cli_reset_extraction(runner: CliRunner, tmp_path: pathlib.Path) -> None:
    """The reset command should clear extraction output while leaving validator records alone."""
    extraction_directory = tmp_path / "extraction"
    extraction_directory.mkdir()
    (extraction_directory / "timestamps.txt").write_text("200101\n")
    records_directory = tmp_path / "records"
    records_directory.mkdir()
    (records_directory / "S3LogAccessExtractor_file-processing-end.txt").write_text("log_a\n")
    (records_directory / "DownloadsLogicPreValidator_abc123.txt").write_text("log_a\n")

    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["reset", "extraction", "--cache", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert list(extraction_directory.rglob(pattern="*")) == []
    assert (records_directory / "S3LogAccessExtractor_file-processing-end.txt").exists() is False
    assert (records_directory / "DownloadsLogicPreValidator_abc123.txt").exists() is True


@pytest.mark.ai_generated
def test_cli_stop_without_running_processes(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """Stopping with nothing running should report as much and leave no stop file behind."""
    monkeypatch.setattr("s3_log_extraction.extractors._stop.get_running_pids", set)

    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["stop", "--cache", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "No extraction processes are currently running." in result.output


@pytest.mark.ai_generated
def test_cli_config_cache_set(runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """Setting the cache directory should persist it to the configuration file."""
    config_file_path = tmp_path / "config.yaml"
    monkeypatch.setattr("s3_log_extraction.config._config.S3_LOG_EXTRACTION_CONFIG_FILE_PATH", config_file_path)
    new_cache_directory = tmp_path / "new_cache"

    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["config", "cache", "set", str(new_cache_directory)])

    assert result.exit_code == 0, result.output
    assert s3_log_extraction.config.get_config() == {"cache_directory": str(new_cache_directory)}
    assert new_cache_directory.is_dir() is True


@pytest.mark.ai_generated
def test_cli_config_tmp_set_and_reset(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """Setting the base temporary directory should persist it, and resetting should clear it again."""
    config_file_path = tmp_path / "config.yaml"
    monkeypatch.setattr("s3_log_extraction.config._config.S3_LOG_EXTRACTION_CONFIG_FILE_PATH", config_file_path)
    new_base_temporary_directory = tmp_path / "new_tmp"

    set_result = runner.invoke(
        s3_log_extraction.s3logextraction_cli, ["config", "tmp", "set", str(new_base_temporary_directory)]
    )

    assert set_result.exit_code == 0, set_result.output
    assert s3_log_extraction.config.get_config() == {"base_temporary_directory": str(new_base_temporary_directory)}
    assert new_base_temporary_directory.is_dir() is True

    reset_result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["config", "tmp", "reset"])

    assert reset_result.exit_code == 0, reset_result.output
    assert s3_log_extraction.config.get_config() == {}


@pytest.mark.ai_generated
def test_cli_extract_with_tmp(runner: CliRunner, tmp_path: pathlib.Path) -> None:
    """The extract command should work under the given base temporary directory and clean up after itself."""
    base_temporary_directory = tmp_path / "tmp"

    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            str(_EXAMPLE_LOGS_DIRECTORY),
            "--workers",
            "2",
            "--cache",
            str(tmp_path),
            "--tmp",
            str(base_temporary_directory),
            "--encryption",
            "false",
        ],
    )

    assert result.exit_code == 0, result.output
    assert base_temporary_directory.is_dir() is True
    assert list(base_temporary_directory.iterdir()) == []


@pytest.mark.ai_generated
def test_cli_extract_in_remote_mode(runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    """In remote mode, the directory argument is the bucket root and every option reaches the remote extractor."""
    captured: dict[str, object] = {}

    class _RecordingRemoteExtractor:
        def __init__(self, **kwargs: object) -> None:
            captured["init"] = kwargs

        def extract_s3_bucket(self, **kwargs: object) -> None:
            captured["extract"] = kwargs

    monkeypatch.setattr(
        "s3_log_extraction._command_line_interface._cli.RemoteS3LogAccessExtractor", _RecordingRemoteExtractor
    )
    inventory_directory = tmp_path / "inventory"
    inventory_directory.mkdir()

    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            "s3://my-logs-bucket",
            "--mode",
            "remote",
            "--limit",
            "7",
            "--workers",
            "1",
            "--cache",
            str(tmp_path),
            "--inventory",
            str(inventory_directory),
            "--encryption",
            "false",
            "--tmp",
            str(tmp_path / "tmp"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["init"] == {
        "cache_directory": tmp_path,
        "use_encryption": False,
        "base_temporary_directory": tmp_path / "tmp",
    }
    assert captured["extract"] == {
        "s3_root": "s3://my-logs-bucket",
        "limit": 7,
        "workers": 1,
        "inventory_directory": str(inventory_directory),
    }


@pytest.mark.ai_generated
@pytest.mark.parametrize("force", [False, True])
def test_cli_update_ip_database(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, force: bool
) -> None:
    """The database command forwards the cache directory and the force flag, and reports where the database is."""
    captured: dict[str, object] = {}
    database_path = tmp_path / "geolite2" / "GeoLite2-City.mmdb"

    def _stub_update_geolite2_database(**kwargs: object) -> pathlib.Path:
        captured.update(kwargs)
        return database_path

    monkeypatch.setattr(
        "s3_log_extraction._command_line_interface._cli.update_geolite2_database", _stub_update_geolite2_database
    )

    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        ["update", "ip", "database", "--cache", str(tmp_path), *(["--force"] if force else [])],
    )

    assert result.exit_code == 0, result.output
    assert captured == {"cache_directory": tmp_path, "force": force}
    assert f"GeoLite2 database is up to date at {database_path}" in result.output


@pytest.mark.ai_generated
def test_cli_testing_generate_benchmark(
    runner: CliRunner, monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path
) -> None:
    """The benchmark command hands the directory argument to the generator."""
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        "s3_log_extraction._command_line_interface._cli.generate_benchmark",
        lambda **kwargs: captured.update(kwargs),
    )

    result = runner.invoke(s3_log_extraction.s3logextraction_cli, ["testing", "generate", "benchmark", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert captured == {"directory": str(tmp_path)}


@pytest.mark.ai_generated
def test_cli_extract_with_limit(runner: CliRunner, tmp_path: pathlib.Path) -> None:
    """The extract command should honor the limit option."""
    result = runner.invoke(
        s3_log_extraction.s3logextraction_cli,
        [
            "extract",
            str(_EXAMPLE_LOGS_DIRECTORY),
            "--limit",
            "1",
            "--workers",
            "1",
            "--cache",
            str(tmp_path),
            "--encryption",
            "false",
        ],
    )

    assert result.exit_code == 0, result.output

    end_record_file_path = tmp_path / "records" / "S3LogAccessExtractor_file-processing-end.txt"

    assert len(end_record_file_path.read_text().splitlines()) == 1

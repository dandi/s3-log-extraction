"""Tests for the ExtractionHeuristicPreValidator."""

import pathlib
import subprocess

import pytest

from s3_log_extraction.validate._extraction_heuristic_pre_validator import (
    ExtractionHeuristicPreValidator,
)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("excluded_ips", "expected_excluded_ips"),
    [
        pytest.param(None, frozenset({"192.0.2.1"}), id="defaults_to_configured_excluded_ips"),
        pytest.param([], frozenset(), id="empty_overrides_configured_excluded_ips"),
        pytest.param(["198.51.100.2"], frozenset({"198.51.100.2"}), id="explicit_overrides_configured_excluded_ips"),
    ],
)
def test_excluded_ips_source(
    monkeypatch: pytest.MonkeyPatch, excluded_ips: list[str] | None, expected_excluded_ips: frozenset[str]
) -> None:
    monkeypatch.setattr(
        "s3_log_extraction.validate._extraction_heuristic_pre_validator.get_excluded_ips",
        lambda: frozenset({"192.0.2.1"}),
    )

    validator = ExtractionHeuristicPreValidator(excluded_ips)
    assert validator.excluded_ips == expected_excluded_ips


@pytest.mark.ai_generated
def test_run_validation_passes_excluded_ips_env(monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path) -> None:
    captured_env: dict[str, str] = {}

    def _run_stub(
        *, args: str, shell: bool, capture_output: bool, text: bool, env: dict[str, str]
    ) -> subprocess.CompletedProcess:
        assert args.startswith("awk --file ")
        assert shell is True
        assert capture_output is True
        assert text is True
        captured_env.update(env)
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("subprocess.run", _run_stub)

    log_path = tmp_path / "test.log"
    log_path.write_text("")

    validator = ExtractionHeuristicPreValidator(["198.51.100.2", "192.0.2.1"])
    validator._run_validation(file_path=log_path)
    assert captured_env == {"EXCLUDED_IPS": "192.0.2.1 198.51.100.2"}


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("excluded_ips", "expected_to_raise"),
    [
        pytest.param([], True, id="aberrant_line_checked"),
        pytest.param(["192.0.2.1"], False, id="aberrant_line_of_excluded_ip_skipped"),
        pytest.param(["192.0.2.10"], True, id="neighboring_address_not_skipped"),
    ],
)
def test_run_validation_skips_only_exact_excluded_ips(
    tmp_path: pathlib.Path, excluded_ips: list[str], expected_to_raise: bool
) -> None:
    """Only lines from an exactly listed address are skipped, unlike a regex that could match its neighbors."""
    log_path = tmp_path / "aberrant.log"
    log_path.write_text(
        "abc123 bucket [01/Jan/2020:00:00:00 +0000] 192.0.2.1 - REQ123 REST.GET.OBJECT test/file.dat "
        '"GET /test/file.dat HTTP/2" 200 - 10 10 "-" "TestAgent" -\n'
    )

    validator = ExtractionHeuristicPreValidator(excluded_ips)

    if expected_to_raise:
        with pytest.raises(RuntimeError, match="Extraction heuristic pre-check failed"):
            validator._run_validation(file_path=log_path)
    else:
        validator._run_validation(file_path=log_path)

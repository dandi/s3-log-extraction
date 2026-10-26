import collections.abc
import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation
from ..config import get_excluded_ips


class ExtractionHeuristicPreValidator(BaseValidator):
    """
    This is an independent pre-check that ensures our fast extraction heuristic does not miss unintended lines.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable

    Parameters
    ----------
    excluded_ips : iterable of str, optional
        The IP addresses whose lines are skipped by the check, matching what extraction drops.
        Defaults to `s3_log_extraction.config.get_excluded_ips()`.
    """

    tqdm_description = "Pre-validating extraction heuristic"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self, excluded_ips: collections.abc.Iterable[str] | None = None, /) -> None:
        self.excluded_ips = frozenset(excluded_ips) if excluded_ips is not None else get_excluded_ips()

        # TODO: does this hold after bundling?
        self._relative_awk_script_path = (
            pathlib.Path(__file__).parent / "_extraction_heuristic_pre_validator_script.awk"
        )

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        _run_awk_validation(
            script_path=self._relative_awk_script_path,
            file_path=file_path,
            failure_label="Extraction heuristic",
            environment_variables={"EXCLUDED_IPS": " ".join(sorted(self.excluded_ips))},
        )

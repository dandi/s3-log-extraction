import os
import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation


class ExtractionHeuristicPreValidator(BaseValidator):
    """
    This is an independent pre-check that ensures our fast extraction heuristic does not miss unintended lines.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating extraction heuristic"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self) -> None:
        self._excluded_ip_regex = os.environ.get("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX") or "^$"

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
            environment_variables={"EXCLUDED_IP_REGEX": self._excluded_ip_regex},
        )

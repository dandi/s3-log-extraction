import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation


class HttpSplitCountPreValidator(BaseValidator):
    """
    This check ensures that there at most only one occurrence of 'HTTP/1.' in each line of the log file.

    No occurrences is allowed but not checked to correspond to GET requests since that is covered by the
    HttpEmptySplitPreValidator.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating 'HTTP/1.' split count"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self) -> None:
        # TODO: does this hold after bundling?
        self._relative_awk_script_path = pathlib.Path(__file__).parent / "_http_split_count_pre_validator_script.awk"

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        _run_awk_validation(
            script_path=self._relative_awk_script_path,
            file_path=file_path,
            failure_label="HTTP split count",
        )

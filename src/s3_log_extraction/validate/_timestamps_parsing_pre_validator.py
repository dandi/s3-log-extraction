import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation


class TimestampsParsingPreValidator(BaseValidator):
    """
    Validate that the timestamp parsing rule results in expected string lengths.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating timestamp parsing"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self) -> None:
        # TODO: does this hold after bundling?
        self._relative_awk_script_path = pathlib.Path(__file__).parent / "_timestamps_parsing_pre_validator_script.awk"

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        _run_awk_validation(
            script_path=self._relative_awk_script_path,
            file_path=file_path,
            failure_label="Timestamps parsing",
        )

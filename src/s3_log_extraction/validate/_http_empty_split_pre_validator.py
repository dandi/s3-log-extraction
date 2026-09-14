import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation


class HttpEmptySplitPreValidator(BaseValidator):
    """
    This check ensures that the "HTTP/1." split rule does not fail to split any line of request type `REST.GET.OBJECT`.

    Note that the request type is extracted by a separate direct space-based split rule.

    TODO: should add pre-validator that the 8th element of each space split is always one of the known types.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating 'HTTP/1.' empty splits"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self) -> None:
        # TODO: does this hold after bundling?
        self._relative_awk_script_path = pathlib.Path(__file__).parent / "_http_empty_split_pre_validator_script.awk"

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        _run_awk_validation(
            script_path=self._relative_awk_script_path,
            file_path=file_path,
            failure_label="HTTP empty split",
        )

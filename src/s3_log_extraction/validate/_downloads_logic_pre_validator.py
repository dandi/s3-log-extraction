import pathlib

from ._base_validator import BaseValidator, _hash_awk_script_file, _run_awk_validation


class DownloadsLogicPreValidator(BaseValidator):
    """
    Pre-validator that checks for aberrant log lines where bytes sent is less than the object size yet status is 200.

    A 200 HTTP status code indicates a complete download, so bytes sent should equal the total object size.
    Any line where bytes sent is a valid number, is less than the total bytes (object size), and the status
    code is exactly 200 is considered aberrant and will cause this validation to fail.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating downloads field logic"

    def __hash__(self) -> int:
        return _hash_awk_script_file(self._relative_awk_script_path)

    # TODO: parallelize
    def __init__(self) -> None:
        # TODO: does this hold after bundling?
        self._relative_awk_script_path = pathlib.Path(__file__).parent / "_downloads_logic_pre_validator_script.awk"

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        """
        Run the downloads logic validation on a single log file.

        Parameters
        ----------
        file_path : pathlib.Path
            The path to the raw S3 log file to validate.

        Raises
        ------
        RuntimeError
            If any log line has a 200 status code but bytes sent is less than the total object size.
        """
        _run_awk_validation(
            script_path=self._relative_awk_script_path,
            file_path=file_path,
            failure_label="Downloads logic",
        )

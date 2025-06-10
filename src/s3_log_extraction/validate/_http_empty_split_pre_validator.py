import hashlib
import pathlib
import subprocess

from ._base_validator import BaseValidator


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
        with self._relative_awk_script_path.open("rb") as file_stream:
            byte_content = file_stream.read()

        checksum = hashlib.sha1(string=byte_content).hexdigest()
        checksum_int = int(checksum, 16)
        return checksum_int

    # TODO: parallelize
    def __init__(self):
        # TODO: does this hold after bundling?
        self._relative_awk_script_path = pathlib.Path(__file__).parent / "_http_empty_split_pre_validator_script.awk"

        super().__init__()

    def _run_validation(self, file_path: pathlib.Path) -> None:
        absolute_awk_script_path = str(self._relative_awk_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        awk_command = f"awk --file {absolute_awk_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            message = (
                f"\nHTTP empty split pre-check failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

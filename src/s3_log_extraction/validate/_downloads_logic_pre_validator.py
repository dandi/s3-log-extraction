import hashlib
import pathlib
import subprocess

from ._base_validator import BaseValidator
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption_utils import decrypt_bytes


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
        """
        Compute a hash based on the contents of the AWK validation script.

        Returns
        -------
        int
            Integer hash derived from the SHA-1 checksum of the AWK script file.
        """
        with self._relative_awk_script_path.open("rb") as file_stream:
            byte_content = file_stream.read()

        checksum = hashlib.sha1(string=byte_content).hexdigest()
        checksum_int = int(checksum, 16)
        return checksum_int

    # TODO: parallelize
    def __init__(self):
        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

        # TODO: does this hold after bundling?
        self._relative_awk_script_path = (
            pathlib.Path(__file__).parent / "_downloads_logic_pre_validator_script.awk"
        )

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
        absolute_awk_script_path = str(self._relative_awk_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        awk_command = f"awk --file {absolute_awk_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env={"DROGON_IP_REGEX": self.DROGON_IP_REGEX},
        )
        if result.returncode != 0:
            message = (
                f"\nDownloads logic pre-check failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

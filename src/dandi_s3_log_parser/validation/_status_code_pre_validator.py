import hashlib
import pathlib
import subprocess

from ._base_validator import BaseValidator
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption import decrypt_bytes


class StatusCodePreValidator(BaseValidator):
    """
    This is an independent pre-check that ensures our fast extraction heuristic does not miss unintended lines.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - resumable
    """

    tqdm_description = "Pre-validating status codes: "

    def _get_code_checksum(self) -> str:
        with self._relative_script_path.open("rb") as file_stream:
            byte_content = file_stream.read()

        validation_rule_checksum = hashlib.sha1(string=byte_content).hexdigest()
        return validation_rule_checksum

    # TODO: parallelize
    def __init__(self):
        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_status_code_pre_validator_script.awk"

        super().__init__()

        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

    def _run_validation(self, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        log_file_path = str(file_path.absolute())

        awk_command = f"awk --file {absolute_script_path} {log_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env={"DROGON_IP_REGEX": self.DROGON_IP_REGEX},
        )
        if result.returncode != 0:
            message = (
                f"\nStatus code pre-check failed.\n "
                f"Log file: {log_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

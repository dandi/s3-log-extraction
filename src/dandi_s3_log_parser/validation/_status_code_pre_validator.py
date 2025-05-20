import pathlib
import subprocess

from ._base_validator import BaseValidator
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption import decrypt_bytes


class StatusCodePreValidator(BaseValidator):
    """
    This is an independent pre-check that ensures our fast extraction heuristic does not miss unintended lines.

    This validator is:
      - not parallelized (planned).
      - interruptible
      - resumable
    """

    tqdm_description = "Pre-validating status codes: "

    # TODO: parallelize
    def __init__(self):
        super().__init__()

        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

    def _run_validation(self, file_path: pathlib.Path) -> None:
        # TODO: will this hold after bundling?
        relative_script_path = pathlib.Path(__file__).parent / "_status_code_pre_validator_script.awk"
        absolute_script_path = str(relative_script_path.absolute())
        log_file_path = str(file_path.absolute())

        result = subprocess.run(
            args=["awk", "--file", absolute_script_path, log_file_path],
            shell=True,
            capture_output=True,
            text=True,
            env={"DROGON_IP_REGEX": self.DROGON_IP_REGEX},
        )
        if result.returncode != 0:
            message = (
                f"\nStatus code pre-check failed.\n "
                f"Log file: {file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

import hashlib
import os
import pathlib
import subprocess

from ._base_validator import BaseValidator
from ..utils.encryption import decrypt_bytes

EXCLUDED_IP_REGEX_ENCRYPTED = (
    b"gAAAAABoLL5Cln6TyMSaPfd8Cu_EIDDnIg2I7R3i-eipDcKGr0DRHXfqIGoxO36CQhEyp4aPR0Ylxu8dF"
    b"OKknTAICvDg7GV33y6dI8d1-C6GsBoSdihP2IYEMwUwasa_dYUEtuTRVz10B0TpZkocjuRPW-CfIPVDgF"
    b"yVXF8AfFESS-yRiL5nueuYsoD6MlJHmHhX0PVRVef6"
)


class ExtractionHeuristicPreValidator(BaseValidator):
    """
    This is an independent pre-check that ensures our fast extraction heuristic does not miss unintended lines.

    This validator is:
      - not parallelized, but could be
      - interruptible
      - updatable
    """

    tqdm_description = "Pre-validating extraction heuristic"

    def _get_excluded_ip_regex(self) -> str:
        encrypt_ip_regex = os.environ.get("S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX", "true").lower() not in [
            "0",
            "false",
            "no",
        ]
        if encrypt_ip_regex:
            return decrypt_bytes(encrypted_data=EXCLUDED_IP_REGEX_ENCRYPTED)

        excluded_ip_regex = os.environ.get("S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX")
        if excluded_ip_regex is None:
            message = "Set S3_LOG_EXTRACTION_EXCLUDED_IP_REGEX when " "S3_LOG_EXTRACTION_ENCRYPT_IP_REGEX is false."
            raise EnvironmentError(message)

        return excluded_ip_regex

    def __hash__(self) -> int:
        with self._relative_awk_script_path.open("rb") as file_stream:
            byte_content = file_stream.read()

        checksum = hashlib.sha1(string=byte_content).hexdigest()
        checksum_int = int(checksum, 16)
        return checksum_int

    # TODO: parallelize
    def __init__(self):
        self.EXCLUDED_IP_REGEX = self._get_excluded_ip_regex()

        # TODO: does this hold after bundling?
        self._relative_awk_script_path = (
            pathlib.Path(__file__).parent / "_extraction_heuristic_pre_validator_script.awk"
        )

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
            env={"EXCLUDED_IP_REGEX": self.EXCLUDED_IP_REGEX},
        )
        if result.returncode != 0:
            message = (
                f"\nExtraction heuristic pre-check failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

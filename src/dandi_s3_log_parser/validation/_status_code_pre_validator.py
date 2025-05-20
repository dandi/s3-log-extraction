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
        self.awk_script = (
            "awk - F'\"' '{"
            '   split($1, pre_uri_fields, " ");'
            ""
            # Pre-URI fields like this should be unaffected
            "   ip = pre_uri_fields[5];"
            f"  if (ip ~ / {self.DROGON_IP_REGEX} /) {{next}};"
            ""
            "   request_type = pre_uri_fields[8];"
            '   if (request_type != "REST.GET.OBJECT") {next};'
            ""
            # Use special rule to try to get reliable status, even in extreme cases
            r"  if ($0 ~ / HTTP\/1\.1 /) {"
            '       split($0, direct_http_split, "HTTP/1.1");'
            '       split(direct_http_split[2], direct_http_space_split," ");'
            "       status_from_direct_rule = direct_http_space_split[2];"
            r"  } else if ($0 ~ / HTTP\/1\.0 /) {"
            '       split($0, direct_http_split, "HTTP/1.0");'
            '       split(direct_http_split[2], direct_http_space_split, " ");'
            "       status_from_direct_rule = direct_http_space_split[2];"
            "   } else {"
            '       print "Line contained neither HTTP/1.1 or HTTP/1.0 - line #" NR " of " FILENAME > "/dev/stderr";'
            '       print $0 > "/dev/stderr";'
            "       exit 1;"
            "   }"
            "   if (status_from_direct_rule !~ / ^[1-5][0-9]{2}$ /) {"
            '       print "Error with direct status code detection - line #" NR " of " FILENAME > "/dev/stderr";'
            '       print "Direct: " status_from_direct_rule > "/dev/stderr";'
            '       print $0 > "/dev/stderr";'
            "       exit 1;"
            "   }"
            # Post-URI fields are more likely to be affected
            '   split($3, post_uri_fields, " ");'
            "   status_from_extraction_rule = post_uri_fields[1];"
            '   is_status_success = substr(status_from_direct_rule, 1, 1) == "2";'
            "   if (status_from_extraction_rule !~ / ^[1-5][0-9]{2}$ / && is_status_success) {"
            '       print "A directly detected success status code was discovered while the extraction rule failed to '
            'detect at all - line #" NR " of " FILENAME > "/dev/stderr";'
            '       print "Extraction: " status_from_extraction_rule > "/dev/stderr";'
            '       print "Direct: " status_from_direct_rule > "/dev/stderr";'
            '       print $0 > "/dev/stderr";'
            "       exit 1;"
            "   }"
            "   if (status_from_extraction_rule != status_from_direct_rule && is_status_success) {"
            '    print "Both status codes were extracted as valid numbers, the direct extraction was successful, but '
            'the two did not match - line #" NR " of " FILENAME > "/dev/stderr";'
            '       print "Extraction: " status_from_extraction_rule > "/dev/stderr";'
            '       print "Direct: " status_from_direct_rule > "/dev/stderr";'
            '       print $0 > "/dev/stderr";'
            "       exit 1;"
            "   }"
            "}'"
        )

    def _run_validation(self, file_path: pathlib.Path) -> None:
        # awk_command = self.awk_script + f" {file_path.absolute()!s}"

        awk_script_path = str(pathlib.Path(__file__).absolute())
        log_file_path = str(file_path.absolute())
        result = subprocess.run(
            args=["awk", "-v", f"drogon_ip_regex={self.DROGON_IP_REGEX}", "-f", awk_script_path, log_file_path],
            shell=True,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            message = (
                f"\nStatus code pre-check failed.\n "
                f"Log file: {file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

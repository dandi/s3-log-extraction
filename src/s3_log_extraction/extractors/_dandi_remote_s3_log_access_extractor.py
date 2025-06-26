import pathlib
import sys

from ._remote_s3_log_access_extractor import RemoteS3LogAccessExtractor
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption_utils import decrypt_bytes


class DandiRemoteS3LogAccessExtractor(RemoteS3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in remotely stored raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - interruptible
          However, you must do so in one of two ways:
            - Invoke the command `s3logextraction stop` to end the processes after the current round of completion.
            - Manually create a file in the extraction cache called '.stop_extraction'.
      - updatable
    """

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        super().__init__(cache_directory=cache_directory)

        awk_filename = "_dandi_extraction.awk" if sys.platform != "win32" else "_dandi_extraction_windows.awk"
        self._relative_script_path = pathlib.Path(__file__).parent / awk_filename

        ips_to_skip_regex = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)
        self._awk_env["IPS_TO_SKIP_REGEX"] = ips_to_skip_regex.decode("utf-8")

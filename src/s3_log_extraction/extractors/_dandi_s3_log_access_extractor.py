import pathlib

from ._s3_log_access_extractor import S3LogAccessExtractor
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption_utils import decrypt_bytes


class DandiS3LogAccessExtractor(S3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - semi-interruptible; most of the computation via AWK can be interrupted safely, but not the mirror copy step
      - updatable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        super().__init__(cache_directory=cache_directory)

        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_dandi_extraction.awk"

        ips_to_skip_regex = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)
        self._awk_env["IPS_TO_SKIP_REGEX"] = ips_to_skip_regex.decode("utf-8")

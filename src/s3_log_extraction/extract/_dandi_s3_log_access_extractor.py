import pathlib
import typing

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

    def __new__(cls, cache_directory: str | pathlib.Path | None = None) -> typing.Self:
        return super().__new__(cls, cache_directory=cache_directory)

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        ips_to_skip_regex = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)
        super().__init__(cache_directory=cache_directory, ips_to_skip_regex=ips_to_skip_regex)

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_extraction.awk"

        initial_mirror_record_difference = {}
        if self.mirror_copy_start_record_file_path.exists() and self.mirror_copy_end_record_file_path.exists():
            with self.mirror_copy_start_record_file_path.open(mode="r") as file_stream:
                mirror_copy_start_record = set(file_stream.read().splitlines())
            with self.mirror_copy_end_record_file_path.open(mode="r") as file_stream:
                mirror_copy_end_record = set(file_stream.read().splitlines())
            initial_mirror_record_difference = mirror_copy_start_record - mirror_copy_end_record
        if len(initial_mirror_record_difference) > 0:
            message = (
                "Mirror copy corruption from previous run detected - "
                "please call `.purge_cache()` to clean the extraction cache and records.\n"
            )
            raise ValueError(message)

        self.extraction_record = {}
        if not self.extraction_record_file_path.exists():
            return

        with self.extraction_record_file_path.open(mode="r") as file_stream:
            self.extraction_record = {line: True for line in file_stream.read().splitlines()}

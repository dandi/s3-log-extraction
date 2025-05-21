import os
import pathlib
import shutil
import subprocess

import tqdm

from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..config import get_cache_directory
from ..encryption import decrypt_bytes


class S3LogAccessExtractor:
    """
    An extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for the DANDI Archive. The information is then reduced to a smaller storage size for further
    processing.

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.

        Must follow the structure:

        <directory>
            ├── YYYY
            │   ├── MM
            │   │   ├── DD.log
            │   │   └── ...
            │   └── ...
            └── ...

    This extractor is:
      - not parallelized, but could be
      - interruptible
      - resumable
    """

    def __init__(self, log_directory: str | pathlib.Path) -> None:
        """
        An extractor of basic access information contained in raw S3 logs.

        This class is not a full parser of all fields but instead is optimized for targeting the most relevant
        information for the DANDI Archive. The information is then reduced to a smaller storage size for further
        processing.

        Parameters
        ----------
        log_directory : path-like
            The directory containing the raw S3 log files to be processed.

            Must follow the structure:

            <directory>
                ├── YYYY
                │   ├── MM
                │   │   ├── DD.log
                │   │   └── ...
                │   └── ...
                └── ...
        """
        self.log_directory = pathlib.Path(log_directory)

        self.cache_directory = get_cache_directory()

        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)
        self.blobs_directory = self.extraction_directory / "blobs"
        self.blobs_directory.mkdir(exist_ok=True)
        self.zarr_directory = self.extraction_directory / "zarr"
        self.zarr_directory.mkdir(exist_ok=True)

        # TODO: might have to be done inside subfunction used by other parallel processes
        self.temporary_directory = self.cache_directory / "tmp" / os.getgid()
        self.temporary_directory.parent.mkdir(exist_ok=True)
        self.temporary_directory.mkdir(exist_ok=True)

        self.record_directory = self.cache_directory / "extraction_records"
        self.record_directory.mkdir(exist_ok=True)

        validation_rule_checksum = "abc"  # TODO
        self.validator_record_file = self.record_directory / f"{validation_rule_checksum}.txt"

        # NOTE: the script below is not true 'parsing'
        # If it fails on a given line no error is raised or tracked and the line is effectively skipped
        # We proceed therefore under the assumption that this rule works only on well-formed lines
        # And that any line it fails on would also have an error status code as a result
        # TODO: needs a verification procedure to ensure
        self.timestamp_file_path = self.temporary_directory / "timestamps.txt"
        self.ip_file_path = self.temporary_directory / "ips.txt"
        self.object_key_file_path = self.temporary_directory / "blobs.txt"
        self.bytes_file_path = self.temporary_directory / "bytes.txt"

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_extractor.awk"

        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

    def _run_extraction(self, file_path: pathlib.Path) -> None:
        year = file_path.parent.parent.name
        month = file_path.parent.name
        record_file_path = self.record_directory / year / month / file_path.name
        record_file_path.parent.mkdir(parents=True, exist_ok=True)

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
        # This part not parallelized
        # with open(file=self.timestamp_file_path, mode="r") as file_stream:
        #     timestamps = file_stream.readlines()
        # with open(file=self.ip_file_path, mode="r") as file_stream:
        #     ips = file_stream.readlines()
        # with open(file=self.object_key_file_path, mode="rb") as file_stream:
        #     object_keys = [self._sanitize_object_key(object_key=object_key) for object_key in file_stream.readlines()]
        # with open(file=self.bytes_file_path, mode="r") as file_stream:
        #     bytes_sent = file_stream.readlines()

        # unique_object_keys = set(object_keys) - {""}
        # for object_key in unique_object_keys:
        #     extraction_file_path

        record_file_path.touch()
        shutil.rmtree(self.temporary_directory)

    @staticmethod
    def _sanitize_object_key(object_key_bytes: bytes) -> str | None:
        """
        Sanitize the raw bytes for the object key to remove any unwanted parts.

        Parameters
        ----------
        object_key : bytes
            The object key to be sanitized, in bytes.

        Returns
        -------
        str or None
            The sanitized object key, decoded as a string.
            Returns None if the object key is neither 'zarr' nor 'blobs'.
        """
        if object_key_bytes[:4] == b"zarr":
            object_key = object_key_bytes[:41].decode("utf-8")
            return object_key
        elif object_key_bytes[:5] == b"blobs":
            object_key = (object_key_bytes[:6] + object_key_bytes[14:]).decode("utf-8")
            return object_key

        return None

    def _record_success(self, file_path: pathlib.Path) -> None:
        """To avoid needlessly rerunning the validation process, we record the file path in a cache file."""
        with self.extractor_record_file.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        file_path = pathlib.Path(file_path)
        absolute_path = str(file_path.absolute())
        if self.record.get(absolute_path, False) is True:
            return

        self._run_validation(file_path=file_path)

        self.record[absolute_path] = True
        self._record_success(file_path=file_path)

    def extract_directory(self, directory: str | pathlib.Path, limit: int | None = None) -> None:
        directory = pathlib.Path(directory)

        all_log_files = {str(file_path.absolute()) for file_path in directory.rglob("*.log")}
        unextracted_files = all_log_files - set(self.record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files
        for file_path in tqdm.tqdm(
            iterable=files_to_extract, desc="Running extraction on S3 logs: ", total=len(files_to_extract), unit="files"
        ):
            self.extract_file(file_path=file_path)

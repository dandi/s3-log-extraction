import collections
import datetime
import os
import pathlib
import subprocess

import numpy
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
      - not parallelized, but could be (though interaction with IP cache could be problematic)
      - interruptible
      - resumable
    """

    def __init__(self, log_directory: str | pathlib.Path) -> None:
        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_extractor.awk"

        self.cache_directory = get_cache_directory()

        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)

        # TODO: might have to be done inside subfunction used by other parallel processes
        self.temporary_directory = self.cache_directory / "tmp" / os.getgid()
        self.temporary_directory.mkdir(parents=True, exist_ok=True)

        self.object_keys_file_path = self.temporary_directory / "object_keys.txt"
        self.timestamps_file_path = self.temporary_directory / "timestamps.txt"
        self.bytes_sent_file_path = self.temporary_directory / "bytes.txt"
        self.ips_file_path = self.temporary_directory / "ips.txt"

        self.record_directory = self.cache_directory / "extraction_records"
        self.record_directory.mkdir(exist_ok=True)

        record_file_name = f"{self.__class__.__name__}.txt"  # NOTE: not hashing the code of the class here yet
        self.record_file_path = self.record_directory / record_file_name  # (Clear cache and results as needed)

        self.record = {}
        if not self.record_file_path.exists():
            return

        with self.record_file_path.open(mode="r") as file_stream:
            self.record = {line: True for line in file_stream.readlines()}

    def _run_extraction(self, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        awk_command = f"awk --file {absolute_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env={"DROGON_IP_REGEX": self.DROGON_IP_REGEX, "TEMPORARY_DIRECTORY": self.temporary_directory},
        )
        if result.returncode != 0:
            message = (
                f"\nExtraction failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

        # with open(file=self.object_keys_file_path, mode="r") as file_stream:
        #     # object_keys = [
        #     self._sanitize_object_key(object_key=object_key) for object_key in file_stream.readlines()
        #     ]
        #     object_keys = file_stream.readlines()
        # with open(file=self.timestamps_file_path, mode="r") as file_stream:
        #     timestamps = [datetime.datetime.strptime(line, "%d/%b/%Y:%H:%M:%S") for line in file_stream.readlines()]
        # with open(file=self.bytes_sent_file_path, mode="rb") as file_stream:
        #     all_bytes_sent = [int(line) for line in file_stream.readlines()]
        # with open(file=self.ips_file_path, mode="rb") as file_stream:
        #     ips = file_stream.readlines()

        object_keys = numpy.loadtxt(fname=self.object_keys_file_path, dtype=str, delimiter="\n")
        timestamps = numpy.loadtxt(
            fname=self.timestamps_file_path,
            dtype=str,
            delimiter="\n",
            converters={0: lambda line: datetime.datetime.strptime(line.decode("utf-8"), "%d/%b/%Y:%H:%M:%S")},
        )
        all_bytes_sent = numpy.loadtxt(fname=self.bytes_sent_file_path, dtype=int, delimiter="\n")
        ips = numpy.loadtxt(fname=self.ips_file_path, dtype=str, delimiter="\n")

        timestamps_per_object_key = collections.defaultdict(list)
        bytes_sent_per_object_key = collections.defaultdict(list)
        ips_per_object_key = collections.defaultdict(list)

        for object_key, timestamp, bytes_sent, ip in zip(object_keys, timestamps, all_bytes_sent, ips):
            timestamps_per_object_key[object_key].append(timestamp.strftime("%y%m%d%H%M%S"))
            bytes_sent_per_object_key[object_key].append(bytes_sent)
            ips_per_object_key[object_key].append(ip)

        # Coregister IPs with anonymized IDs
        # (and somehow randomize to avoid biases based on usage patterns)

        # TODO
        for object_key in timestamps_per_object_key.keys():
            mirror_file_path = self.extraction_directory / f"{object_key}.txt"
            mirror_file_path.mkdir(parents=True, exist_ok=True)

            with mirror_file_path.open(mode="a") as file_stream:
                file_stream.writelines(
                    lines=[
                        f"{timestamp}\t{bytes_sent}\t{ip}\n"
                        for timestamp, bytes_sent, ip in zip(
                            timestamps_per_object_key[object_key],
                            bytes_sent_per_object_key[object_key],
                            ips_per_object_key[object_key],
                        )
                    ]
                )

        # TODO: re-enable cleanup when done testing
        # shutil.rmtree(self.temporary_directory)

    # TODO: might want to remove; and just mirror whatever object structure happens to have been on the bucket
    # @staticmethod
    # def _sanitize_object_key(object_key_bytes: bytes) -> str | None:
    #     """
    #     Sanitize the raw bytes for the object key to remove any unwanted parts.
    #
    #     Parameters
    #     ----------
    #     object_key : bytes
    #         The object key to be sanitized, in bytes.
    #
    #     Returns
    #     -------
    #     str or None
    #         The sanitized object key, decoded as a string.
    #         Returns None if the object key is neither 'zarr' nor 'blobs'.
    #     """
    #     if object_key_bytes[:4] == b"zarr":
    #         object_key = object_key_bytes[:41].decode("utf-8")
    #         return object_key
    #     elif object_key_bytes[:5] == b"blobs":
    #         object_key = (object_key_bytes[:6] + object_key_bytes[14:]).decode("utf-8")
    #         return object_key
    #
    #     return None

    # TODO: shouldn't this be absolute file path (str)?
    def _record_success(self, file_path: pathlib.Path) -> None:
        """To avoid needlessly rerunning the validation process, we record the file path in a cache file."""
        with self.record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.record.get(absolute_file_path, False) is True:
            return

        self._run_extraction(file_path=file_path)

        self.record[absolute_file_path] = True
        self._record_success(file_path=file_path)

    def extract_directory(self, directory: str | pathlib.Path, limit: int | None = None) -> None:
        directory = pathlib.Path(directory)

        all_log_files = {str(file_path.absolute()) for file_path in directory.rglob("*.log")}
        unextracted_files = all_log_files - set(self.record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files
        for file_path in tqdm.tqdm(
            iterable=files_to_extract,
            desc="Running extraction on S3 logs: ",
            total=len(files_to_extract),
            unit="file",
            smoothing=0,
        ):
            self.extract_file(file_path=file_path)

import collections
import concurrent.futures
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

    This extractor is:
      - parallelized
      - semi-interruptible; most of the computation via AWK can be interrupted safely, but the cache mirror step cannot
      - resumable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    def __init__(self) -> None:
        self.DROGON_IP_REGEX = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_extraction.awk"

        self.cache_directory = get_cache_directory()

        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)

        self.base_temporary_directory = self.cache_directory / "tmp"
        self.base_temporary_directory.mkdir(exist_ok=True)

        self.extraction_record_directory = self.cache_directory / "extraction_records"
        self.extraction_record_directory.mkdir(exist_ok=True)

        self.file_copy_start_record_directory = self.cache_directory / "file_copy_start_records"
        self.file_copy_start_record_directory.mkdir(exist_ok=True)

        self.file_copy_end_record_directory = self.cache_directory / "file_copy_end_records"
        self.file_copy_end_record_directory.mkdir(exist_ok=True)

        record_file_name = f"{self.__class__.__name__}.txt"  # NOTE: not hashing the code of the class here yet
        self.extraction_record_file_path = self.extraction_record_directory / record_file_name
        self.file_copy_start_record_file_path = self.file_copy_start_record_directory / record_file_name
        self.file_copy_end_record_file_path = self.file_copy_end_record_directory / record_file_name

        self.extraction_record = {}
        if not self.extraction_record_file_path.exists():
            return

        with self.extraction_record_file_path.open(mode="r") as file_stream:
            self.extraction_record = {line.strip(): True for line in file_stream.readlines()}

    def _run_extraction(self, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        absolute_temporary_directory = str(self.temporary_directory.absolute())
        awk_command = f"awk --file {absolute_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env={"DROGON_IP_REGEX": self.DROGON_IP_REGEX, "TEMPORARY_DIRECTORY": absolute_temporary_directory},
        )
        if result.returncode != 0:
            message = (
                f"\nExtraction failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

        object_keys = numpy.loadtxt(fname=self.object_keys_file_path, dtype=str)
        with self.timestamps_file_path.open(mode="r") as file_stream:
            parsed_timestamps = [
                datetime.datetime.strptime(line.strip(), "%d/%b/%Y:%H:%M:%S") for line in file_stream.readlines()
            ]
        timestamps = numpy.array(parsed_timestamps)
        all_bytes_sent = numpy.loadtxt(fname=self.bytes_sent_file_path, dtype="uint64")
        ips = numpy.loadtxt(fname=self.ips_file_path, dtype=str)

        timestamps_per_object_key = collections.defaultdict(list)
        bytes_sent_per_object_key = collections.defaultdict(list)
        ips_per_object_key = collections.defaultdict(list)

        for object_key, timestamp, bytes_sent, ip in zip(object_keys, timestamps, all_bytes_sent, ips):
            timestamps_per_object_key[object_key].append(timestamp.isoformat())
            bytes_sent_per_object_key[object_key].append(bytes_sent)
            ips_per_object_key[object_key].append(ip)

        with self.file_copy_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

        for object_key in timestamps_per_object_key.keys():
            mirror_directory = self.extraction_directory / object_key
            mirror_directory.parent.mkdir(parents=True, exist_ok=True)

            timestamps_mirror_file_path = mirror_directory / "timestamps.txt"
            with timestamps_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=timestamps_per_object_key[object_key])
            bytes_sent_mirror_file_path = mirror_directory / "bytes_sent.txt"
            with bytes_sent_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=bytes_sent_per_object_key[object_key])
            ips_mirror_file_path = mirror_directory / "ips.txt"
            with ips_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=ips_per_object_key[object_key])

        with self.file_copy_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

        # TODO: re-enable cleanup when done testing
        # shutil.rmtree(self.temporary_directory)

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.extraction_record.get(absolute_file_path, False) is True:
            return

        # These must be set per process
        self.temporary_directory = self.base_temporary_directory / str(os.getpid())
        self.temporary_directory.mkdir(exist_ok=True)
        self.object_keys_file_path = self.temporary_directory / "object_keys.txt"
        self.timestamps_file_path = self.temporary_directory / "timestamps.txt"
        self.bytes_sent_file_path = self.temporary_directory / "bytes_sent.txt"
        self.ips_file_path = self.temporary_directory / "ips.txt"

        self._run_extraction(file_path=file_path)

        self.extraction_record[absolute_file_path] = True
        with self.extraction_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")

    def extract_directory(
        self, directory: str | pathlib.Path, limit: int | None = None, max_workers: int | None = None
    ) -> None:
        directory = pathlib.Path(directory)

        all_log_files = {str(file_path.absolute()) for file_path in directory.rglob("*.log")}
        unextracted_files = all_log_files - set(self.extraction_record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files

        if max_workers is None or max_workers == 1:
            for file_path in tqdm.tqdm(
                iterable=files_to_extract,
                desc="Running extraction on S3 logs: ",
                total=len(files_to_extract),
                unit="file",
                smoothing=0,
            ):
                self.extract_file(file_path=file_path)
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                list(
                    tqdm.tqdm(
                        executor.map(self.extract_file, map(str, files_to_extract)),
                        desc="Running extraction on S3 logs: ",
                        total=len(files_to_extract),
                        unit="file",
                        smoothing=0,
                    )
                )

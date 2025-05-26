import collections
import concurrent.futures
import datetime
import os
import pathlib
import shutil
import subprocess
import sys
import time
import typing

import numpy
import tqdm

from ..config import get_cache_directory


class S3LogAccessExtractor:
    """
    An extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - interruptible; BUT you must do so by creating a file called 'stop_extraction' in the extraction record directory
      - updatable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    @classmethod
    def _get_cache_directories(cls, cache_directory: pathlib.Path | None = None) -> None:
        """
        Create the cache directory and subdirectories if they do not exist.
        """
        cls.cache_directory = cache_directory or get_cache_directory()

        cls.extraction_directory = cls.cache_directory / "extraction"
        cls.extraction_directory.mkdir(exist_ok=True)

        cls.base_temporary_directory = cls.cache_directory / "tmp"
        cls.base_temporary_directory.mkdir(exist_ok=True)

        cls.extraction_record_directory = cls.cache_directory / "extraction_records"
        cls.extraction_record_directory.mkdir(exist_ok=True)

        extraction_record_file_name = f"{cls.__class__.__name__}_extraction.txt"
        cls.extraction_record_file_path = cls.extraction_record_directory / extraction_record_file_name
        cls.interrupt_file_path = cls.extraction_record_directory / "stop_extraction"
        mirror_copy_start_record_file_name = f"{cls.__class__.__name__}_mirror-copy-start.txt"
        cls.mirror_copy_start_record_file_path = cls.extraction_record_directory / mirror_copy_start_record_file_name
        mirror_copy_end_record_file_name = f"{cls.__class__.__name__}_mirror-copy-end.txt"
        cls.mirror_copy_end_record_file_path = cls.extraction_record_directory / mirror_copy_end_record_file_name

    @classmethod
    def reset_cache(cls) -> None:
        """
        Purge the cache directory and all extraction records.
        """
        cls._get_cache_directories()

        shutil.rmtree(path=cls.extraction_directory)
        shutil.rmtree(path=cls.base_temporary_directory)
        cls.extraction_record_file_path.unlink(missing_ok=True)
        cls.mirror_copy_start_record_file_path.unlink(missing_ok=True)
        cls.mirror_copy_end_record_file_path.unlink(missing_ok=True)

    def __new__(
        cls, cache_directory: str | pathlib.Path | None = None, ips_to_skip_regex: str | None = None
    ) -> typing.Self:
        cache_directory = pathlib.Path(cache_directory) if cache_directory is not None else None
        cls._get_cache_directories(cache_directory=cache_directory)

        return super().__new__(cls)

    def __init__(self, cache_directory: pathlib.Path | None = None, ips_to_skip_regex: str | None = None) -> None:
        # AWK is not as readily available on Windows
        if sys.platform == "win32":
            awk_path = pathlib.Path.home() / "anaconda3" / "Library" / "usr" / "bin" / "awk.exe"

            if not awk_path.exists():
                message = "Unable to find `awk`, which is required for extraction - please raise an issue."
                raise RuntimeError(message)
        self.awk_base = "awk" if sys.platform != "win32" else awk_path

        # Long-term TODO: use a separate script with no IP filtering for even further speedup in case of " "
        self.ips_to_skip_regex = ips_to_skip_regex or " "

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
                "please call `.reset_cache()` to clean the extraction cache and records.\n"
            )
            raise ValueError(message)

        self.extraction_record = {}
        if not self.extraction_record_file_path.exists():
            return

        with self.extraction_record_file_path.open(mode="r") as file_stream:
            self.extraction_record = {line: True for line in file_stream.read().splitlines()}

    def _run_extraction(self, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        absolute_temporary_directory = str(self.temporary_directory.absolute()) + str(pathlib.Path("/"))
        awk_command = f"{self.awk_base} --file {absolute_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env={
                "IPS_TO_SKIP_REGEX": self.ips_to_skip_regex,
                "TEMPORARY_DIRECTORY": absolute_temporary_directory,
            },
        )
        if result.returncode != 0:
            message = (
                f"\nExtraction failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

        # Sometimes a log file (especially very early ones) may not have any valid GET entries
        if not self.object_keys_file_path.exists():
            return

        # Record the start of the mirror copy step
        with self.mirror_copy_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

        timestamps_per_object_key = collections.defaultdict(list)
        bytes_sent_per_object_key = collections.defaultdict(list)
        ips_per_object_key = collections.defaultdict(list)

        # Mirror the timestamps
        object_keys = numpy.loadtxt(fname=self.object_keys_file_path, dtype=str)
        unique_object_keys = numpy.unique(ar=object_keys)
        for object_key in unique_object_keys:
            mirror_directory = self.extraction_directory / object_key
            mirror_directory.mkdir(parents=True, exist_ok=True)

        with self.timestamps_file_path.open(mode="r") as file_stream:
            all_timestamps = [
                datetime.datetime.strptime(line.strip(), "%d/%b/%Y:%H:%M:%S").strftime(format="%y%m%d%H%M%S")
                for line in file_stream.readlines()
            ]

        for object_key, timestamp in zip(object_keys, all_timestamps):
            timestamps_per_object_key[object_key].append(timestamp)

        for object_key, timestamps in timestamps_per_object_key.items():
            mirror_directory = self.extraction_directory / object_key
            timestamps_mirror_file_path = mirror_directory / "timestamps.txt"
            with timestamps_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=timestamps_per_object_key[object_key], fmt="%s")
        del timestamps, timestamps_per_object_key  # Clear memory to reduce overhead

        # Mirror the bytes sent
        all_bytes_sent = numpy.loadtxt(fname=self.bytes_sent_file_path, dtype="uint64")
        for object_key, bytes_sent in zip(object_keys, all_bytes_sent):
            bytes_sent_per_object_key[object_key].append(bytes_sent)

        for object_key, bytes_sent in bytes_sent_per_object_key.items():
            mirror_directory = self.extraction_directory / object_key
            bytes_sent_mirror_file_path = mirror_directory / "bytes_sent.txt"
            with bytes_sent_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=bytes_sent, fmt="%d")
        del all_bytes_sent, bytes_sent_per_object_key

        # Mirror the IPs
        all_ips = numpy.loadtxt(fname=self.ips_file_path, dtype="U15")
        for object_key, ip in zip(object_keys, all_ips):
            ips_per_object_key[object_key].append(ip)

        for object_key, ips in ips_per_object_key.items():
            mirror_directory = self.extraction_directory / object_key
            ips_mirror_file_path = mirror_directory / "full_ips.txt"
            with ips_mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=ips, fmt="%s")

        # Record final success and cleanup
        with self.mirror_copy_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")
        shutil.rmtree(path=self.temporary_directory)

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        while self.interrupt_file_path.exists() is True:
            print("Extraction interrupted - waiting for the interrupt file to be removed...")
            time.sleep(60)

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
        self.ips_file_path = self.temporary_directory / "full_ips.txt"

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

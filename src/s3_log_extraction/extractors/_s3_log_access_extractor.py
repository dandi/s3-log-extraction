import collections
import concurrent.futures
import os
import pathlib
import shutil
import subprocess
import sys
import typing

import natsort
import numpy
import tqdm

from ..config import get_cache_directory, get_extraction_directory, get_records_directory, get_temporary_directory


class S3LogAccessExtractor:
    """
    An extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - interruptible
          However, you must do so in one of two ways:
            - Create a file in the records cache called 'stop_extraction' to end the processes after current completion.
      - updatable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    def __init__(self, *, cache_directory: pathlib.Path | None = None) -> None:
        # AWK is not as readily available on Windows
        if sys.platform == "win32":
            awk_path = pathlib.Path.home() / "anaconda3" / "Library" / "usr" / "bin" / "awk.exe"

            if not awk_path.exists():
                message = "Unable to find `awk`, which is required for extraction - please raise an issue."
                raise RuntimeError(message)
        self.awk_base = "awk" if sys.platform != "win32" else awk_path

        self.cache_directory = cache_directory or get_cache_directory()
        self.extraction_directory = get_extraction_directory(cache_directory=self.cache_directory)
        self.base_temporary_directory = get_temporary_directory(cache_directory=self.cache_directory)

        # Special file for safe interruption during parallel extraction
        self.records_directory = get_records_directory(cache_directory=self.cache_directory)
        self.stop_file_path = self.records_directory / "stop_extraction"

        class_name = self.__class__.__name__
        extraction_record_file_name = f"{class_name}_extraction.txt"
        self.extraction_record_file_path = self.records_directory / extraction_record_file_name
        mirror_copy_start_record_file_name = f"{class_name}_mirror-copy-start.txt"
        self.mirror_copy_start_record_file_path = self.records_directory / mirror_copy_start_record_file_name
        mirror_copy_end_record_file_name = f"{class_name}_mirror-copy-end.txt"
        self.mirror_copy_end_record_file_path = self.records_directory / mirror_copy_end_record_file_name

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_generic_extraction.awk"
        self._awk_env = dict()

        initial_mirror_record_difference = dict()
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

    def _run_extraction(self, *, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        absolute_temporary_directory = str(self.temporary_directory.absolute()) + str(pathlib.Path("/"))
        self._awk_env["TEMPORARY_DIRECTORY"] = absolute_temporary_directory

        awk_command = f"{self.awk_base} --file {absolute_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=awk_command,
            shell=True,
            capture_output=True,
            text=True,
            env=self._awk_env,
        )
        if result.returncode != 0:
            message = (
                f"\nExtraction failed.\n "
                f"Log file: {absolute_file_path}\n"
                f"Error code {result.returncode}\n\n"
                f"stderr: {result.stderr}\n"
            )
            raise RuntimeError(message)

    def _bin_and_save_extracted_data(
        self,
        *,
        object_keys: typing.Iterable[str],
        all_data: typing.Iterable[str | int],
        filename: str,
        write_format: typing.Literal["%d", "%s"],
    ) -> None:
        data_per_object_key = collections.defaultdict(list)
        for object_key, data in zip(object_keys, all_data):
            data_per_object_key[object_key].append(data)

        for object_key, data in data_per_object_key.items():
            mirror_directory = self.extraction_directory / object_key
            mirror_file_path = mirror_directory / filename
            with mirror_file_path.open(mode="a") as file_stream:
                numpy.savetxt(fname=file_stream, X=data, fmt=write_format)

    def _mirror_copy(self) -> None:
        # Mirror the timestamps
        object_keys = numpy.loadtxt(fname=self.object_keys_file_path, dtype=str)

        unique_object_keys = numpy.unique(ar=object_keys)
        for object_key in unique_object_keys:
            mirror_directory = self.extraction_directory / object_key
            mirror_directory.mkdir(parents=True, exist_ok=True)
        del unique_object_keys  # Clear memory to reduce overhead

        all_timestamps = numpy.loadtxt(fname=self.timestamps_file_path, dtype="uint64")
        self._bin_and_save_extracted_data(
            object_keys=object_keys,
            all_data=all_timestamps,
            filename="timestamps.txt",
            write_format="%d",
        )
        del all_timestamps

        all_bytes_sent = numpy.loadtxt(fname=self.bytes_sent_file_path, dtype="uint64")
        self._bin_and_save_extracted_data(
            object_keys=object_keys,
            all_data=all_bytes_sent,
            filename="bytes_sent.txt",
            write_format="%d",
        )
        del all_bytes_sent

        all_ips = numpy.loadtxt(fname=self.ips_file_path, dtype="U15")
        self._bin_and_save_extracted_data(
            object_keys=object_keys,
            all_data=all_ips,
            filename="full_ips.txt",
            write_format="%s",
        )
        del all_ips

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        pid = str(os.getpid())
        if self.stop_file_path.exists() is True:
            print(f"Extraction stopped on process {pid} - exiting...")
            return

        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.extraction_record.get(absolute_file_path, False) is True:
            return

        self.temporary_directory = self.base_temporary_directory / pid
        self.temporary_directory.mkdir(exist_ok=True)
        self.object_keys_file_path = self.temporary_directory / "object_keys.txt"
        self.timestamps_file_path = self.temporary_directory / "timestamps.txt"
        self.bytes_sent_file_path = self.temporary_directory / "bytes_sent.txt"
        self.ips_file_path = self.temporary_directory / "full_ips.txt"

        self._run_extraction(file_path=file_path)

        # Sometimes a log file (especially very early ones) may not have any valid GET entries
        if not self.object_keys_file_path.exists():
            return

        # Record the start of the mirror copy step
        with self.mirror_copy_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")

        self._mirror_copy()

        # Record final success and cleanup
        with self.mirror_copy_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")
        shutil.rmtree(path=self.temporary_directory)

        self.extraction_record[absolute_file_path] = True
        with self.extraction_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")

    def extract_directory(
        self, *, directory: str | pathlib.Path, limit: int | None = None, max_workers: int = 1
    ) -> None:
        directory = pathlib.Path(directory)

        all_log_files = {
            str(file_path.absolute()) for file_path in natsort.natsorted(seq=directory.rglob(pattern="*.log"))
        }
        unextracted_files = all_log_files - set(self.extraction_record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files

        if max_workers == 1:
            for file_path in tqdm.tqdm(
                iterable=files_to_extract,
                total=len(files_to_extract),
                desc="Running extraction on S3 logs: ",
                unit="file",
                smoothing=0,
                mininterval=10.0,  # in seconds
            ):
                self.extract_file(file_path=file_path)
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                list(
                    tqdm.tqdm(
                        iterable=executor.map(self.extract_file, map(str, files_to_extract)),
                        total=len(files_to_extract),
                        desc="Running extraction on S3 logs: ",
                        unit="file",
                        smoothing=0,
                    )
                )

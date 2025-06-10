import os
import pathlib
import subprocess
import sys

import natsort
import tqdm

from ..config import get_cache_directory, get_extraction_directory, get_records_directory


class S3LogAccessExtractor:
    """
    An extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - not parallelized; to do so would require a synchronized file appender at the AWK level (also RAM constraints)
      - interruptible
          However, you must do so in one of two ways:
            - Invoke the command `s3logextraction stop` to end the processes after the current round of completion.
            - Manually create a file in the records cache called 'stop_extraction'.
      - updatable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    def __init__(self, *, cache_directory: pathlib.Path | None = None) -> None:
        # AWK is not as readily available on Windows
        if sys.platform == "win32":
            awk_path = pathlib.Path.home() / "anaconda3" / "Library" / "usr" / "bin" / "gawk.exe"

            if not awk_path.exists():
                message = "Unable to find `gawk`, which is required for extraction - please raise an issue."
                raise RuntimeError(message)
        self.gawk_base = "gawk" if sys.platform != "win32" else awk_path

        self.cache_directory = cache_directory or get_cache_directory()
        self.extraction_directory = get_extraction_directory(cache_directory=self.cache_directory)
        self.stop_file_path = self.extraction_directory / "stop_extraction"

        # Special file for safe interruption during parallel extraction
        self.records_directory = get_records_directory(cache_directory=self.cache_directory)

        class_name = self.__class__.__name__
        file_processing_start_record_file_name = f"{class_name}_file-processing-start.txt"
        self.file_processing_start_record_file_path = self.records_directory / file_processing_start_record_file_name
        file_processing_end_record_file_name = f"{class_name}_file-processing-end.txt"
        self.file_processing_end_record_file_path = self.records_directory / file_processing_end_record_file_name

        # TODO: does this hold after bundling?
        awk_filename = "_generic_extraction.awk" if sys.platform != "win32" else "_generic_extraction_windows.awk"
        self._relative_script_path = pathlib.Path(__file__).parent / awk_filename
        self._awk_env = {"EXTRACTION_DIRECTORY": str(self.extraction_directory)}

        self.file_processing_end_record = set()
        file_processing_record_difference = dict()
        if self.file_processing_start_record_file_path.exists() and self.file_processing_end_record_file_path.exists():
            with self.file_processing_start_record_file_path.open(mode="r") as file_stream:
                file_processing_start_record = set(file_stream.read().splitlines())
            with self.file_processing_end_record_file_path.open(mode="r") as file_stream:
                self.file_processing_end_record = set(file_stream.read().splitlines())
            file_processing_record_difference = file_processing_start_record - self.file_processing_end_record
        if len(file_processing_record_difference) > 0:
            # TODO: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3_log_extraction.reset_extraction()` to clean the extraction cache and records.\n\n"
            )
            raise ValueError(message)

    def _run_extraction(self, *, file_path: pathlib.Path) -> None:
        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        gawk_command = f"{self.gawk_base} --file {absolute_script_path} {absolute_file_path}"
        result = subprocess.run(
            args=gawk_command,
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
                f"stderr: {result.stderr}\n\n"
            )
            raise RuntimeError(message)

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        pid = str(os.getpid())
        if self.stop_file_path.exists() is True:
            print(f"Extraction stopped on process {pid} - exiting...")
            return

        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.file_processing_end_record.get(absolute_file_path, False) is True:
            return

        # Record the start of the mirror copy step
        with self.file_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")

        self._run_extraction(file_path=file_path)

        # Record final success and cleanup
        self.file_processing_end_record[absolute_file_path] = True
        with self.file_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{absolute_file_path}\n")

    def extract_directory(self, *, directory: str | pathlib.Path, limit: int | None = None) -> None:
        directory = pathlib.Path(directory)

        all_log_files = {
            str(file_path.absolute()) for file_path in natsort.natsorted(seq=directory.rglob(pattern="*.log"))
        }
        unextracted_files = all_log_files - self.file_processing_end_record

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files

        for file_path in tqdm.tqdm(
            iterable=files_to_extract,
            total=len(files_to_extract),
            desc="Running extraction on S3 logs: ",
            unit="file",
            smoothing=0,
            miniters=1,
        ):
            self.extract_file(file_path=file_path)

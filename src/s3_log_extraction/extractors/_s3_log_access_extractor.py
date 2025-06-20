import concurrent.futures
import os
import pathlib
import subprocess
import sys
import warnings

import natsort
import tqdm

from ._globals import _STOP_EXTRACTION_FILE_NAME
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
          However, you must use the command `s3logextraction stop` to end the processes after the current completion.
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
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME

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

        self.file_processing_end_record = dict()
        file_processing_record_difference = dict()
        if self.file_processing_start_record_file_path.exists() and self.file_processing_end_record_file_path.exists():
            file_processing_start_record = set(
                file_path for file_path in self.file_processing_start_record_file_path.read_text().splitlines()
            )
            self.file_processing_end_record = {
                file_path: True for file_path in self.file_processing_end_record_file_path.read_text().splitlines()
            }
            file_processing_record_difference = file_processing_start_record - set(
                self.file_processing_end_record.keys()
            )
        if len(file_processing_record_difference) > 0:
            # TODO: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3_log_extraction reset extraction` to clean the extraction cache and records.\n\n"
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

    def extract_directory(self, *, directory: str | pathlib.Path, limit: int | None = None, workers: int = -2) -> None:
        directory = pathlib.Path(directory)
        if workers == 0:
            message = "The number of workers cannot be 0 - please set it to an integer. Falling back to default of -2."
            warnings.warn(message=message, stacklevel=2)
            workers = -2
        cpu_count = os.cpu_count()
        max_workers = workers % cpu_count + 1 if workers < 0 else max(cpu_count, workers)

        all_log_files = {
            str(file_path.absolute()) for file_path in natsort.natsorted(seq=directory.rglob(pattern="*-*-*-*-*-*-*"))
        }
        unextracted_files = all_log_files - set(self.file_processing_end_record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files

        tqdm_style_kwargs = {
            "total": len(files_to_extract),
            "desc": "Running extraction on S3 logs: ",
            "unit": "files",
            "smoothing": 0,
            "miniters": 1,
        }
        if max_workers == 1:
            for file_path in tqdm.tqdm(iterable=files_to_extract, **tqdm_style_kwargs):
                self.extract_file(file_path=file_path)
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                list(
                    tqdm.tqdm(iterable=executor.map(self.extract_file, map(str, files_to_extract)), **tqdm_style_kwargs)
                )

import concurrent.futures
import pathlib
import sys

import natsort
import tqdm

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import _deploy_subprocess, _handle_gawk_base, _handle_max_workers
from ..config import get_cache_directory, get_extraction_directory, get_records_directory


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
          However, you must use the command `s3logextraction stop` to end the processes after the current completion.
      - updatable
    """

    def __init__(self, *, cache_directory: pathlib.Path | None = None) -> None:
        self.gawk_base = _handle_gawk_base()

        self.cache_directory = cache_directory or get_cache_directory()
        self.extraction_directory = get_extraction_directory(cache_directory=self.cache_directory)
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME
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
        file_processing_record_difference = set()
        if self.file_processing_start_record_file_path.exists() and self.file_processing_end_record_file_path.exists():
            file_processing_start_record = {
                file_path for file_path in self.file_processing_start_record_file_path.read_text().splitlines()
            }
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
        _deploy_subprocess(
            command=gawk_command,
            environment_variables=self._awk_env,
            error_message=f"Extraction failed on {file_path}.",
        )

    def extract_file(self, file_path: str | pathlib.Path) -> None:
        if self.stop_file_path.exists() is True:
            return

        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.file_processing_end_record.get(absolute_file_path, False) is True:
            return

        # Record the start of the mirror copy step
        content = f"{absolute_file_path}\n"
        with self.file_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

        self._run_extraction(file_path=file_path)

        # Record final success and cleanup
        self.file_processing_end_record[absolute_file_path] = True
        with self.file_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

    def extract_directory(self, *, directory: str | pathlib.Path, limit: int | None = None, workers: int = -2) -> None:
        directory = pathlib.Path(directory)
        max_workers = _handle_max_workers(workers=workers)

        all_log_files = {
            str(file_path.absolute()) for file_path in natsort.natsorted(seq=directory.rglob(pattern="*-*-*-*-*-*-*"))
        }
        unextracted_files = all_log_files - set(self.file_processing_end_record.keys())

        files_to_extract = list(unextracted_files)[:limit] if limit is not None else unextracted_files

        tqdm_style_kwargs = {
            "total": len(files_to_extract),
            "desc": "Running extraction on S3 logs",
            "unit": "files",
            "smoothing": 0,
        }
        if max_workers == 1:
            for file_path in tqdm.tqdm(iterable=files_to_extract, **tqdm_style_kwargs):
                self.extract_file(file_path=file_path)
        else:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                list(
                    tqdm.tqdm(iterable=executor.map(self.extract_file, map(str, files_to_extract)), **tqdm_style_kwargs)
                )

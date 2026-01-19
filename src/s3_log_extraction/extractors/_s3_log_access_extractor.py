import collections
import concurrent.futures
import itertools
import math
import os
import pathlib
import random
import shutil
import tempfile

import natsort
import tqdm

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import _deploy_subprocess
from .._parallel._utils import _handle_max_workers
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
        self.cache_directory = cache_directory or get_cache_directory()
        self.extraction_directory = get_extraction_directory(cache_directory=self.cache_directory)
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME
        self.records_directory = get_records_directory(cache_directory=self.cache_directory)
        self.temporary_directory = pathlib.Path(tempfile.mkdtemp(prefix="s3logextraction-"))

        class_name = self.__class__.__name__
        file_processing_start_record_file_name = f"{class_name}_file-processing-start.txt"
        self.file_processing_start_record_file_path = self.records_directory / file_processing_start_record_file_name
        file_processing_end_record_file_name = f"{class_name}_file-processing-end.txt"
        self.file_processing_end_record_file_path = self.records_directory / file_processing_end_record_file_name

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_generic_extraction.awk"
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
            # IDEA: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3_log_extraction reset extraction` to clean the extraction cache and records.\n\n"
            )
            raise ValueError(message)

    def extract_directory(
        self,
        *,
        directory: str | pathlib.Path,
        limit: int | None = None,
        workers: int = -2,
        batch_size: int = 5_000,
    ) -> None:
        directory = pathlib.Path(directory)
        max_workers = _handle_max_workers(workers=workers)

        all_log_files = {
            str(file_path.absolute()) for file_path in natsort.natsorted(seq=directory.rglob(pattern="*-*-*-*-*-*-*"))
        }
        unextracted_files = list(all_log_files - set(self.file_processing_end_record.keys()))

        files_to_extract = unextracted_files[:limit] if limit is not None else unextracted_files
        random.shuffle(files_to_extract)

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
            batches = itertools.batched(iterable=files_to_extract, n=batch_size)
            number_of_batches = math.ceil(len(files_to_extract) / batch_size)
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                pid_specific_extraction_directory = pathlib.Path(tempfile.mkdtemp(prefix="s3logextraction-"))
                pid_specific_extraction_directory.mkdir(exist_ok=True)
                self._awk_env["EXTRACTION_DIRECTORY"] = str(pid_specific_extraction_directory)

                for batch in tqdm.tqdm(
                    iterable=batches,
                    total=number_of_batches,
                    desc="Extracting in batches",
                    unit="batches",
                    smoothing=0,
                    position=0,
                    leave=True,
                ):
                    if self.stop_file_path.exists():
                        return

                    tqdm_style_kwargs["total"] = len(batch)
                    futures = [
                        executor.submit(self.extract_file, file_path=file_path, enable_stop=False, parallel_mode=True)
                        for file_path in batch
                    ]
                    collections.deque(
                        (
                            future.result()
                            for future in tqdm.tqdm(
                                iterable=concurrent.futures.as_completed(futures),
                                position=1,
                                leave=False,
                                **tqdm_style_kwargs,
                            )
                        ),
                        maxlen=0,
                    )

                    files_to_copy = list(self.temporary_directory.rglob(pattern="*.txt"))
                    for file_path in tqdm.tqdm(
                        iterable=files_to_copy,
                        total=len(files_to_copy),
                        desc="Copying files from child processes",
                        unit="files",
                        smoothing=0,
                        position=1,
                        leave=False,
                    ):
                        relative_parts = file_path.relative_to(self.temporary_directory).parts[1:]
                        relative_file_path = pathlib.Path(*relative_parts)
                        destination_file_path = self.extraction_directory / relative_file_path
                        destination_file_path.parent.mkdir(parents=True, exist_ok=True)

                        content = file_path.read_bytes()
                        with destination_file_path.open(mode="ab") as file_stream:
                            file_stream.write(content)
                        file_path.unlink()

        shutil.rmtree(path=self.temporary_directory, ignore_errors=True)

    def extract_file(
        self, file_path: str | pathlib.Path, enable_stop: bool = True, parallel_mode: bool = False
    ) -> None:
        if enable_stop is True and self.stop_file_path.exists() is True:
            return

        # Wish I didn't have to ensure this per job
        extraction_directory = None
        if parallel_mode is True:
            extraction_directory = self.temporary_directory / str(os.getpid())
            extraction_directory.mkdir(exist_ok=True)

        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if self.file_processing_end_record.get(absolute_file_path, False) is True:
            return

        # Record the start of the mirror copy step
        content = f"{absolute_file_path}\n"
        with self.file_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

        self._run_extraction(file_path=file_path, extraction_directory=extraction_directory)

        # Record final success and cleanup
        self.file_processing_end_record[absolute_file_path] = True
        with self.file_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

    def _run_extraction(self, *, file_path: pathlib.Path, extraction_directory: pathlib.Path | None = None) -> None:
        if extraction_directory is not None:
            self._awk_env["EXTRACTION_DIRECTORY"] = str(extraction_directory)

        absolute_script_path = str(self._relative_script_path.absolute())
        absolute_file_path = str(file_path.absolute())

        gawk_command = f"gawk --file {absolute_script_path} {absolute_file_path}"
        _deploy_subprocess(
            command=gawk_command,
            environment_variables=self._awk_env,
            error_message=f"Extraction failed on {file_path}.",
        )

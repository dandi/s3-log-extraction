import collections
import collections.abc
import concurrent.futures
import functools
import itertools
import math
import os
import pathlib
import random
import shutil

import natsort
import tqdm

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import (
    _create_temporary_directory,
    _merge_dir_to_extraction,
    _merge_worker_output_into_extraction,
    _run_awk_extraction,
)
from ..config import (
    get_base_temporary_directory,
    get_cache_directory,
    get_cache_subdirectory,
    get_excluded_ips,
)
from ..utils import _handle_max_workers


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
          However, you must use the command `s3logextraction stop` to end the processes safely as soon as possible.
      - updatable

    Parameters
    ----------
    cache_directory : pathlib.Path | None, optional
        The directory holding the extraction cache and its records.
        Defaults to the configured cache directory.
    use_encryption : bool, optional
        Whether to encrypt IP addresses in the extraction output. Defaults to `True`.
    base_temporary_directory : pathlib.Path | None, optional
        The base directory to create this run's temporary directory inside.
        Defaults to the configured base temporary directory, and to the system temporary directory when
        none is configured.
    excluded_ips : iterable of str, optional
        The IP addresses whose requests are dropped during extraction, so they never enter the cache.
        Defaults to `s3_log_extraction.config.get_excluded_ips()`, the addresses listed in
        `~/.s3-log-extraction/excluded_ips.txt`.
        An explicit value replaces that default, so pass an empty list to drop nothing.
    """

    def __init__(
        self,
        *,
        cache_directory: pathlib.Path | None = None,
        use_encryption: bool = True,
        base_temporary_directory: pathlib.Path | None = None,
        excluded_ips: collections.abc.Iterable[str] | None = None,
    ) -> None:
        self.cache_directory = cache_directory or get_cache_directory()
        self.use_encryption = use_encryption
        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME
        self.records_directory = get_cache_subdirectory(cache_directory=self.cache_directory, name="records")
        self.base_temporary_directory = base_temporary_directory or get_base_temporary_directory()

        class_name = self.__class__.__name__
        file_processing_start_record_file_name = f"{class_name}_file-processing-start.txt"
        self.file_processing_start_record_file_path = self.records_directory / file_processing_start_record_file_name
        file_processing_end_record_file_name = f"{class_name}_file-processing-end.txt"
        self.file_processing_end_record_file_path = self.records_directory / file_processing_end_record_file_name

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_generic_extraction.awk"
        self.excluded_ips = frozenset(excluded_ips) if excluded_ips is not None else get_excluded_ips()
        self._awk_env = {
            "EXTRACTION_DIRECTORY": str(self.extraction_directory),
            "EXCLUDED_IPS": " ".join(sorted(self.excluded_ips)),
        }

        self.file_processing_end_record: set[str] = set()
        file_processing_record_difference: set[str] = set()
        if self.file_processing_start_record_file_path.exists() and self.file_processing_end_record_file_path.exists():
            file_processing_start_record = set(self.file_processing_start_record_file_path.read_text().splitlines())
            self.file_processing_end_record = set(self.file_processing_end_record_file_path.read_text().splitlines())
            file_processing_record_difference = file_processing_start_record - self.file_processing_end_record
        if len(file_processing_record_difference) > 0:
            # IDEA: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3logextraction reset extraction` to clean the extraction cache and records.\n\n"
            )
            raise ValueError(message)

    @functools.cached_property
    def temporary_directory(self) -> pathlib.Path:
        """
        The temporary directory of this run, created inside the base temporary directory on first use.

        It is created lazily so that an extractor which only checks for pending work never makes one.
        """
        temporary_directory = _create_temporary_directory(self.base_temporary_directory)
        return temporary_directory

    def has_unextracted_files(self, *, directory: str | pathlib.Path) -> bool:
        """
        Check whether any log files in a directory have not yet been extracted, without extracting them.

        The directory is only walked up to the first log file that has not been extracted. This only reads the
        records of this extractor and writes nothing.

        Parameters
        ----------
        directory : str or pathlib.Path
            The path to the folder containing all raw S3 log files, as passed to `extract_directory`.

        Returns
        -------
        bool
            Whether at least one log file has not yet been extracted.
        """
        directory = pathlib.Path(directory)
        has_unextracted_files = any(
            str(file_path.relative_to(directory)) not in self.file_processing_end_record
            for file_path in directory.rglob(pattern="*-*-*-*-*-*-*")
        )

        return has_unextracted_files

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

        all_relative_log_paths = {
            str(file_path.relative_to(directory))
            for file_path in natsort.natsorted(seq=directory.rglob(pattern="*-*-*-*-*-*-*"))
        }
        unextracted_relative_paths = list(all_relative_log_paths - self.file_processing_end_record)
        # Resolve back to absolute paths for the actual extraction
        unextracted_files = [str((directory / rel_path).absolute()) for rel_path in unextracted_relative_paths]

        files_to_extract = unextracted_files[:limit] if limit is not None else unextracted_files
        random.shuffle(files_to_extract)

        # Created before any worker starts, so that every worker shares the same directory
        temporary_directory = self.temporary_directory

        tqdm_style_kwargs = {
            "total": len(files_to_extract),
            "desc": "Running extraction on S3 logs",
            "unit": "files",
            "smoothing": 0,
        }
        if max_workers == 1:
            for file_path in tqdm.tqdm(iterable=files_to_extract, **tqdm_style_kwargs):
                self.extract_file(file_path=file_path, log_root=directory)
        else:
            batches = itertools.batched(iterable=files_to_extract, n=batch_size)
            number_of_batches = math.ceil(len(files_to_extract) / batch_size)
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                # No directory is made for the pool itself: every worker overrides EXTRACTION_DIRECTORY
                # with its own process-ID directory under `temporary_directory` before it runs the AWK script.
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
                        executor.submit(
                            self.extract_file,
                            file_path=file_path,
                            enable_stop=False,
                            parallel_mode=True,
                            log_root=directory,
                        )
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

                    files_to_copy = list(temporary_directory.rglob(pattern="*.txt"))
                    _merge_worker_output_into_extraction(
                        files_to_copy=files_to_copy,
                        temporary_directory=temporary_directory,
                        extraction_directory=self.extraction_directory,
                        use_encryption=self.use_encryption,
                    )

        shutil.rmtree(path=temporary_directory, ignore_errors=True)

    def extract_file(
        self,
        file_path: str | pathlib.Path,
        enable_stop: bool = True,
        parallel_mode: bool = False,
        log_root: pathlib.Path | None = None,
    ) -> None:
        if enable_stop is True and self.stop_file_path.exists() is True:
            return

        # Wish I didn't have to ensure this per job
        extraction_directory = None
        if parallel_mode is True:
            extraction_directory = self.temporary_directory / str(os.getpid())
            extraction_directory.mkdir(exist_ok=True)
        elif self.use_encryption:
            # For single-worker mode with encryption: use a per-call temp dir so we can use_encryption on merge
            extraction_directory = _create_temporary_directory(self.base_temporary_directory)

        file_path = pathlib.Path(file_path)
        if log_root is not None:
            record_key = str(file_path.resolve().relative_to(log_root.resolve()))
        else:
            record_key = str(file_path.absolute())
        if record_key in self.file_processing_end_record:
            return

        # Record the start of the mirror copy step
        content = f"{record_key}\n"
        with self.file_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

        self._run_extraction(file_path=file_path, extraction_directory=extraction_directory)

        if not parallel_mode and self.use_encryption and extraction_directory is not None:
            _merge_dir_to_extraction(
                source_dir=extraction_directory,
                extraction_directory=self.extraction_directory,
                use_encryption=self.use_encryption,
            )
            shutil.rmtree(path=extraction_directory, ignore_errors=True)

        # Record final success and cleanup
        self.file_processing_end_record.add(record_key)
        with self.file_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(content)

    def _run_extraction(self, *, file_path: pathlib.Path, extraction_directory: pathlib.Path | None = None) -> None:
        _run_awk_extraction(
            script_path=self._relative_script_path,
            file_path=file_path,
            awk_env=self._awk_env,
            extraction_directory=extraction_directory,
        )

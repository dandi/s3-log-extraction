import collections
import concurrent.futures
import itertools
import math
import os
import pathlib
import random
import shutil
import tempfile
import warnings

import tqdm
import yaml

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import _deploy_subprocess, _handle_aws_credentials, _merge_dir_to_extraction, _merge_file_into_extraction
from ..config import get_cache_directory, get_cache_subdirectory
from ..utils import _handle_max_workers, _read_s3_urls_from_local_inventory


class RemoteS3LogAccessExtractor:
    """
    Extractor of basic access information contained in remotely stored raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - interruptible
          However, you must do so in one of two ways:
            - Invoke the command `s3logextraction stop` to end the processes after the current round of completion.
            - Manually create a file in the extraction cache called '.stop_extraction'.
      - updatable
    """

    def __init__(self, cache_directory: pathlib.Path | None = None, use_encryption: bool = True) -> None:
        self.cache_directory = cache_directory or get_cache_directory()
        self.use_encryption = use_encryption
        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME
        self.records_directory = get_cache_subdirectory(cache_directory=self.cache_directory, name="records")
        self.temporary_directory = pathlib.Path(tempfile.mkdtemp(prefix="s3logextraction-"))

        class_name = self.__class__.__name__
        s3_url_processing_start_record_file_name = f"{class_name}_s3-url-processing-start.txt"
        self.s3_url_processing_start_record_file_path = (
            self.records_directory / s3_url_processing_start_record_file_name
        )
        s3_url_processing_end_record_file_name = f"{class_name}_s3-url-processing-end.txt"
        self.s3_url_processing_end_record_file_path = self.records_directory / s3_url_processing_end_record_file_name

        # TODO: does this hold after bundling?
        self._relative_script_path = pathlib.Path(__file__).parent / "_generic_extraction.awk"
        self._awk_env = {"EXTRACTION_DIRECTORY": str(self.extraction_directory)}

    def extract_s3_bucket(
        self,
        *,
        s3_root: str,
        limit: int | None = None,
        workers: int = -2,
        batch_size: int = 5_000,
        inventory_directory: str | pathlib.Path | None = None,
    ) -> None:
        """
        Extract S3 log access data from a remote S3 bucket.

        Parameters
        ----------
        s3_root : str
            The root S3 path of the log bucket (e.g. ``s3://my-logs-bucket``).
        limit : int or None, optional
            Maximum number of files to process.  If ``None`` (default), all
            unprocessed files are processed.
        workers : int, optional
            Number of parallel workers.  Negative values use ``cpu_count +
            workers + 1`` cores (e.g. ``-2`` means all-but-one core).
            Defaults to ``-2``.
        batch_size : int, optional
            Number of S3 URLs to dispatch per batch when using multiple
            workers.  Defaults to ``5_000``.
        inventory_directory : str or pathlib.Path or None, optional
            Path to a local pre-downloaded S3 inventory directory.  The
            directory must follow the standard AWS S3 Inventory layout::

                <inventory_directory>/
                ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
                │   ├── manifest.json
                │   └── manifest.checksum
                ├── data/
                │   └── <uuid>.csv.gz    # gzip-compressed CSV inventory files
                └── hive/
                    └── dt=<YYYY-MM-DD-HH-MM>/
                        └── symlink.txt  # references to data/*.csv.gz

            The most recent hive partition is used to determine which data
            files to parse.  Each ``data/*.csv.gz`` file is parsed according
            to the schema in the corresponding ``manifest.json``.  When
            omitted, log files are discovered by scanning the remote bucket
            directly, which can be very slow for large buckets.
        """
        _handle_aws_credentials()
        max_workers = _handle_max_workers(workers=workers)

        unprocessed_s3_urls = self._get_unprocessed_s3_urls(s3_root=s3_root, inventory_directory=inventory_directory)
        s3_urls_to_extract = unprocessed_s3_urls[:limit] if limit is not None else unprocessed_s3_urls

        tqdm_style_kwargs = {
            "desc": "Running extraction on remote S3 logs",
            "unit": "files",
            "smoothing": 0,
        }
        if max_workers == 1:
            for s3_url in tqdm.tqdm(
                iterable=s3_urls_to_extract, total=len(s3_urls_to_extract), leave=True, **tqdm_style_kwargs
            ):
                self._extract_s3_url(s3_url=s3_url, s3_root=s3_root)
        else:
            batches = itertools.batched(iterable=s3_urls_to_extract, n=batch_size)
            number_of_batches = math.ceil(len(s3_urls_to_extract) / batch_size)
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
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
                        shutil.rmtree(path=self.temporary_directory, ignore_errors=True)
                        return

                    tqdm_style_kwargs["total"] = len(batch)
                    futures = [
                        executor.submit(
                            self._extract_s3_url,
                            s3_url=s3_url,
                            enable_stop=False,
                            parallel_mode=True,
                            s3_root=s3_root,
                        )
                        for s3_url in batch
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

                    files_to_copy = [
                        path for path in self.temporary_directory.rglob(pattern="*.txt") if path.is_file() is True
                    ]
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
                        _merge_file_into_extraction(
                            source_file_path=file_path,
                            destination_file_path=destination_file_path,
                            use_encryption=self.use_encryption,
                        )
                        file_path.unlink()
                    shutil.rmtree(path=self.temporary_directory)
                    self.temporary_directory.mkdir()

        shutil.rmtree(path=self.temporary_directory, ignore_errors=True)

    def _get_unprocessed_s3_urls(
        self,
        s3_root: str,
        inventory_directory: pathlib.Path | None = None,
    ) -> list[str]:
        self._get_end_record_and_check_consistency()

        if inventory_directory is not None:
            unprocessed_s3_urls = self._get_unprocessed_s3_urls_from_local_inventory(
                inventory_directory=inventory_directory, s3_root=s3_root
            )
        else:
            unprocessed_s3_urls = self._get_unprocessed_s3_urls_from_remote(s3_root=s3_root)

        del self.s3_url_processing_end_record  # Free memory

        # Randomize the order of the remote files for the progress bar to be more accurate
        random.shuffle(x=unprocessed_s3_urls)

        return unprocessed_s3_urls

    def _get_end_record_and_check_consistency(self) -> None:
        self.s3_url_processing_end_record: set[str] = set()
        s3_url_processing_record_difference: set[str] = set()
        if (
            self.s3_url_processing_start_record_file_path.exists()
            and self.s3_url_processing_end_record_file_path.exists()
        ):
            s3_url_processing_start_record = {
                file_path for file_path in self.s3_url_processing_start_record_file_path.read_text().splitlines()
            }
            self.s3_url_processing_end_record = {
                file_path for file_path in self.s3_url_processing_end_record_file_path.read_text().splitlines()
            }
            s3_url_processing_record_difference = s3_url_processing_start_record - self.s3_url_processing_end_record
        if len(s3_url_processing_record_difference) > 0:
            # IDEA: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3logextraction reset extraction` to clean the extraction cache and records.\n\n"
            )
            raise ValueError(message)

    def _get_unprocessed_s3_urls_from_local_inventory(
        self, inventory_directory: pathlib.Path, s3_root: str
    ) -> list[str]:
        """
        Get unprocessed S3 URLs from a pre-downloaded local AWS S3 Inventory directory.

        The inventory directory must follow the standard AWS S3 Inventory layout,
        with timestamped subdirectories (e.g. ``2026-05-03T01-00Z/``), a ``data/``
        folder containing gzip-compressed CSV files, and a ``hive/`` folder with
        Hive-partitioned symlink files (e.g. ``hive/dt=2026-05-03-01-00/symlink.txt``).

        The most recent hive partition is used to determine which data files to read.

        Parameters
        ----------
        inventory_directory : pathlib.Path
            Path to the pre-downloaded S3 inventory root directory.
        s3_root : str
            The root S3 path used for extraction (e.g. ``s3://my-logs-bucket``).
            Only object keys that fall under this prefix are considered.

        Returns
        -------
        list[str]
            Deduplicated list of S3 URLs that have not yet been processed.

        Raises
        ------
        FileNotFoundError
            If no hive partitions are found or required files are missing.
        ValueError
            If the ``Key`` column is absent from the inventory schema.
        """
        inventory = _read_s3_urls_from_local_inventory(
            inventory_directory=pathlib.Path(inventory_directory),
            s3_root=s3_root,
        )

        unprocessed_dates = list(set(inventory.keys()) - self.processed_dates)

        s3_urls = [url for date in unprocessed_dates for url in inventory[date]]

        unprocessed_s3_urls = [url for url in s3_urls if url.split("/")[-1] not in self.s3_url_processing_end_record]
        return unprocessed_s3_urls

    def _get_unprocessed_s3_urls_from_remote(self, s3_root: str) -> list[str]:
        warnings.warn(
            "Fetching log file listings directly from S3 via network requests can be very slow for large buckets. "
            "Consider setting up AWS S3 Inventory on your bucket and using the `inventory_directory` argument "
            "for significantly better performance.",
            # stacklevel=3 surfaces the warning at the user-facing extract_s3_bucket() call site
            # rather than inside the internal helper chain.
            stacklevel=3,
        )
        years_result = _deploy_subprocess(
            command=f"s5cmd ls {s3_root}/", error_message=f"Failed to scan years of nested structure at {s3_root}."
        )
        years = {line.split(" ")[-1].rstrip("/\n") for line in years_result.splitlines()}
        unprocessed_years = list(years - self.processed_years)

        dates_with_logs = []
        unprocessed_months_per_year = dict()
        for year in unprocessed_years:
            subdirectory = f"{s3_root}/{year}"
            months_result = _deploy_subprocess(
                command=f"s5cmd ls {subdirectory}/", error_message=f"Failed to list structure of {subdirectory}/."
            )
            if months_result is None:
                continue

            months = {f"{line.split(" ")[-1].rstrip("/\n")}" for line in months_result.splitlines()}
            unprocessed_months_per_year[year] = list(months - self.processed_months_per_year.get(year, set()))

            for month in unprocessed_months_per_year[year]:
                subdirectory = f"{s3_root}/{year}/{month}"
                days_result = _deploy_subprocess(
                    command=f"s5cmd ls {subdirectory}/", error_message=f"Failed to list structure of {subdirectory}/."
                )
                if days_result is None:
                    continue

                dates = [f"{year}-{month}-{line.split(" ")[-1].rstrip("/\n")}" for line in days_result.splitlines()]
                dates_with_logs.extend(dates)

        new_dates = list(set(dates_with_logs) - self.processed_dates)
        sorted_new_dates = sorted(list(new_dates))
        unprocessed_dates = sorted_new_dates[:-2]  # Give a 2-day buffer to allow AWS to catch up

        s3_urls = []
        for date in tqdm.tqdm(
            iterable=unprocessed_dates,
            total=len(unprocessed_dates),
            desc="Assembling remote manifest",
            unit="dates",
            smoothing=0,
            miniters=1,
            leave=False,
        ):
            year, month, day = date.split("-")
            subdirectory = f"{s3_root}/{year}/{month}/{day}"
            s3_urls_result = _deploy_subprocess(
                command=f"s5cmd ls {subdirectory}/", error_message=f"Failed to list structure of {subdirectory}/."
            )
            if s3_urls_result is None:
                continue
            s3_urls.extend(
                [f"{subdirectory}/{line.split(" ")[-1].rstrip("\n")}" for line in s3_urls_result.splitlines()]
            )

        unprocessed_s3_urls = [url for url in s3_urls if url.split("/")[-1] not in self.s3_url_processing_end_record]
        return unprocessed_s3_urls

    def _extract_s3_url(
        self,
        s3_url: str,
        enable_stop: bool = True,
        parallel_mode: bool = False,
        s3_root: str | None = None,
    ) -> None:
        import fsspec

        if enable_stop is True and self.stop_file_path.exists():
            return

        # Wish I didn't have to ensure this per job
        extraction_directory = None
        if parallel_mode is True:
            extraction_directory = self.temporary_directory / str(os.getpid())
            extraction_directory.mkdir(exist_ok=True)
        elif self.use_encryption:
            # For single-worker mode with encryption: use a per-call temp dir so we can use_encryption on merge
            extraction_directory = pathlib.Path(tempfile.mkdtemp(prefix="s3logextraction-"))

        record_key = s3_url.split("/")[-1]

        # Record the start of the extraction step
        with self.s3_url_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{record_key}\n")

        temporary_file_path = self.temporary_directory / s3_url.split("/")[-1]
        with fsspec.open(urlpath=s3_url, mode="rb") as file_stream:
            temporary_file_path.write_bytes(data=file_stream.read())

        self._run_extraction(file_path=temporary_file_path, extraction_directory=extraction_directory)

        if not parallel_mode and self.use_encryption and extraction_directory is not None:
            _merge_dir_to_extraction(
                source_dir=extraction_directory,
                extraction_directory=self.extraction_directory,
                use_encryption=self.use_encryption,
            )
            shutil.rmtree(path=extraction_directory, ignore_errors=True)

        # Record final success and cleanup
        with self.s3_url_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{record_key}\n")
        temporary_file_path.unlink()

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

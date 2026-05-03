import collections
import concurrent.futures
import itertools
import json
import math
import os
import pathlib
import random
import shutil
import tempfile

import pydantic
import tqdm
import yaml

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import _deploy_subprocess, _handle_aws_credentials
from .._parallel._utils import _handle_max_workers
from ..config import get_cache_directory, get_extraction_directory, get_records_directory


class RemoteS3LogAccessExtractor:
    """
    Extractor of basic access information contained in remotely stored raw S3 logs.

    This remote access design assumes that the S3 logs are stored in a nested structure. If you still use the flat
    storage pattern, or have a mix of the two structures, you should use the `manifest_file_path` argument
    to `.extract_s3(...)`.

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

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        self.cache_directory = cache_directory or get_cache_directory()
        self.extraction_directory = get_extraction_directory(cache_directory=self.cache_directory)
        self.stop_file_path = self.extraction_directory / _STOP_EXTRACTION_FILE_NAME
        self.records_directory = get_records_directory(cache_directory=self.cache_directory)
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

        self.processed_years: set[str] = set()
        self.processed_years_record_file_path = self.records_directory / "processed_years.yaml"
        if self.processed_years_record_file_path.exists():
            with self.processed_years_record_file_path.open(mode="r") as file_stream:
                loaded = yaml.safe_load(stream=file_stream)
                self.processed_years = set(loaded.keys()) if isinstance(loaded, dict) else set(loaded or [])

        self.processed_months_per_year: dict[str, set[str]] = dict()
        self.processed_months_per_year_record_file_path = self.records_directory / "processed_months_per_year.yaml"
        if self.processed_months_per_year_record_file_path.exists():
            with self.processed_months_per_year_record_file_path.open(mode="r") as file_stream:
                loaded = yaml.safe_load(stream=file_stream) or {}
                self.processed_months_per_year = {
                    year: set(months.keys()) if isinstance(months, dict) else set(months or [])
                    for year, months in loaded.items()
                }

    def extract_s3_bucket(
        self,
        *,
        s3_root: str,
        limit: int | None = None,
        workers: int = -2,
        batch_size: int = 5_000,
        manifest_file_path: str | pathlib.Path | None = None,
        inventory_s3_path: str | None = None,
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
        manifest_file_path : str or pathlib.Path or None, optional
            Path to a local pre-parsed JSON manifest file listing log files
            that would not be discoverable via the natural nested structure
            (e.g. flat-layout legacy files).  Mutually exclusive with
            ``inventory_s3_path`` for the remote-listing path, but both may
            be supplied together to cover both sources.
        inventory_s3_path : str or None, optional
            S3 path to a weekly inventory file (e.g.
            ``s3://my-logs-bucket/inventory.txt``) containing all current log
            object keys, one full S3 URL per line.  When provided, the
            inventory is used in place of live ``s5cmd ls`` calls to discover
            unprocessed log files.
        """
        _handle_aws_credentials()
        max_workers = _handle_max_workers(workers=workers)

        unprocessed_s3_urls = self._get_unprocessed_s3_urls(
            manifest_file_path=manifest_file_path, s3_root=s3_root, inventory_s3_path=inventory_s3_path
        )
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
                self._extract_s3_url(s3_url=s3_url)
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
                        executor.submit(self._extract_s3_url, s3_url=s3_url, enable_stop=False, parallel_mode=True)
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

                        content = file_path.read_bytes()
                        with destination_file_path.open(mode="ab") as file_stream:
                            file_stream.write(content)
                        file_path.unlink()
                    shutil.rmtree(path=self.temporary_directory)
                    self.temporary_directory.mkdir()

        shutil.rmtree(path=self.temporary_directory, ignore_errors=True)

    def _get_unprocessed_s3_urls(
        self,
        manifest_file_path: pathlib.Path | None,
        s3_root: str,
        inventory_s3_path: str | None = None,
    ) -> list[str]:
        self._get_end_record_and_check_consistency()

        self.processed_dates: set[str] = set()
        processed_dates_record_file_path = self.records_directory / "processed_dates.yaml"
        if processed_dates_record_file_path.exists():
            with processed_dates_record_file_path.open(mode="r") as file_stream:
                loaded = yaml.safe_load(stream=file_stream)
                self.processed_dates = set(loaded.keys()) if isinstance(loaded, dict) else set(loaded or [])

        unprocessed_s3_urls_from_manifest = self._get_unprocessed_s3_urls_from_manifest(
            manifest_file_path=manifest_file_path, s3_root=s3_root
        )
        if inventory_s3_path is not None:
            unprocessed_s3_urls_from_inventory_or_remote = self._get_unprocessed_s3_urls_from_inventory(
                inventory_s3_path=inventory_s3_path, s3_root=s3_root
            )
        else:
            unprocessed_s3_urls_from_inventory_or_remote = self._get_unprocessed_s3_urls_from_remote(s3_root=s3_root)
        unprocessed_s3_urls = unprocessed_s3_urls_from_manifest + unprocessed_s3_urls_from_inventory_or_remote

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

    def _get_unprocessed_s3_urls_from_manifest(
        self, manifest_file_path: pathlib.Path | None, s3_root: str
    ) -> list[str]:
        s3_base = "/".join(s3_root.split("/")[:3])

        manifest = dict()
        manifest_file_path = pathlib.Path(manifest_file_path) if manifest_file_path is not None else None
        if manifest_file_path is not None:
            with manifest_file_path.open(mode="r") as file_stream:
                manifest = json.load(fp=file_stream)

        dates_from_manifest = [date for date in manifest.keys()]
        unprocessed_dates = list(set(dates_from_manifest) - self.processed_dates)

        s3_urls = [
            f"{s3_base}/{filename}"
            for date in tqdm.tqdm(
                iterable=unprocessed_dates,
                total=len(unprocessed_dates),
                desc="Assembling local manifest",
                unit="dates",
                smoothing=0,
                miniters=1,
                leave=False,
            )
            for filename in manifest[date]
        ]

        unprocessed_s3_urls = list(set(s3_urls) - self.s3_url_processing_end_record)
        return unprocessed_s3_urls

    def _get_unprocessed_s3_urls_from_inventory(self, inventory_s3_path: str, s3_root: str) -> list[str]:
        """
        Get unprocessed S3 URLs from a weekly inventory file stored in S3.

        The inventory file must contain one full S3 URL per line, e.g.::

            s3://my-logs-bucket/2024/01/01/2024-01-01-00-00-00-ABCDEF

        Parameters
        ----------
        inventory_s3_path : str
            Full S3 path to the inventory text file
            (e.g. ``s3://my-logs-bucket/inventory.txt``).
        s3_root : str
            The root S3 path used for extraction
            (e.g. ``s3://my-logs-bucket``).  Only lines that start with
            this prefix are considered.

        Returns
        -------
        list[str]
            Deduplicated list of S3 URLs that have not yet been processed.
        """
        import fsspec

        with fsspec.open(urlpath=inventory_s3_path, mode="r") as file_stream:
            inventory_content = file_stream.read()

        # Normalize s3_root so we can strip it as a prefix safely.
        # The trailing slash ensures that adjacent prefixes such as
        # "s3://bucket/logs-extra" are not matched when s3_root is
        # "s3://bucket/logs".
        s3_root_prefix = s3_root.rstrip("/") + "/"

        inventory: dict[str, list[str]] = collections.defaultdict(list)
        for raw_line in inventory_content.splitlines():
            url = raw_line.strip()
            if not url or not url.startswith(s3_root_prefix):
                continue
            # Relative path after the root: year/month/day/filename
            relative_path = url[len(s3_root_prefix) :]
            parts = relative_path.split("/")
            if len(parts) < 4:
                continue
            year, month, day = parts[0], parts[1], parts[2]
            date = f"{year}-{month}-{day}"
            inventory[date].append(url)

        unprocessed_dates = list(set(inventory.keys()) - self.processed_dates)

        s3_urls = [
            url
            for date in tqdm.tqdm(
                iterable=unprocessed_dates,
                total=len(unprocessed_dates),
                desc="Assembling inventory",
                unit="dates",
                smoothing=0,
                miniters=1,
                leave=False,
            )
            for url in inventory[date]
        ]

        unprocessed_s3_urls = list(set(s3_urls) - self.s3_url_processing_end_record)
        return unprocessed_s3_urls

    def _get_unprocessed_s3_urls_from_remote(self, s3_root: str) -> list[str]:
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

        unprocessed_s3_urls = list(set(s3_urls) - self.s3_url_processing_end_record)
        return unprocessed_s3_urls

    def _extract_s3_url(
        self,
        s3_url: str,
        enable_stop: bool = True,
        parallel_mode: bool = False,
    ) -> None:
        import fsspec

        if enable_stop is True and self.stop_file_path.exists():
            return

        # Wish I didn't have to ensure this per job
        extraction_directory = None
        if parallel_mode is True:
            extraction_directory = self.temporary_directory / str(os.getpid())
            extraction_directory.mkdir(exist_ok=True)

        # Record the start of the extraction step
        with self.s3_url_processing_start_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{s3_url}\n")

        temporary_file_path = self.temporary_directory / s3_url.split("/")[-1]
        with fsspec.open(urlpath=s3_url, mode="rb") as file_stream:
            temporary_file_path.write_bytes(data=file_stream.read())

        self._run_extraction(file_path=temporary_file_path, extraction_directory=extraction_directory)

        # Record final success and cleanup
        with self.s3_url_processing_end_record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{s3_url}\n")
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

    @staticmethod
    @pydantic.validate_call
    def parse_manifest(*, file_path: pydantic.FilePath) -> None:
        """
        Read the manifest file and save it as a parsed JSON object, adjacent to the initial file.

        The raw manifest file is the output of `s5cmd ls s3_root/* > manifest.txt`.
        """
        manifest = collections.defaultdict(list)
        filenames = [line.split(" ")[-1].strip() for line in file_path.read_text().splitlines() if "DIR" not in line]
        for filename in tqdm.tqdm(
            iterable=filenames,
            total=len(filenames),
            desc="Parsing local manifest",
            unit="files",
            smoothing=0,
            leave=False,
        ):
            filename_splits = filename.split("-")
            year = filename_splits[0]
            month = filename_splits[1]
            day = filename_splits[2]
            date = f"{year}-{month}-{day}"
            manifest[date].append(filename)

        parsed_file_path = file_path.parent / f"{file_path.stem}_parsed.json"
        parsed_file_path.unlink(missing_ok=True)
        with parsed_file_path.open(mode="w") as file_stream:
            json.dump(obj=dict(manifest), fp=file_stream, indent=2)

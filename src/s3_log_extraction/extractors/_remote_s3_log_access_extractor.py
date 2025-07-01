import collections
import concurrent.futures
import itertools
import json
import os
import pathlib
import random
import shutil
import sys
import tempfile

import fsspec
import tqdm
import yaml

from ._globals import _STOP_EXTRACTION_FILE_NAME
from ._utils import _deploy_subprocess, _handle_aws_credentials, _handle_gawk_base, _handle_max_workers
from ..config import get_cache_directory, get_extraction_directory, get_records_directory


class RemoteS3LogAccessExtractor:
    """
    A DANDI-specific extractor of basic access information contained in remotely stored raw S3 logs.

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
        self.gawk_base = _handle_gawk_base()

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
        awk_filename = "_generic_extraction.awk" if sys.platform != "win32" else "_generic_extraction_windows.awk"
        self._relative_script_path = pathlib.Path(__file__).parent / awk_filename
        self._awk_env = {"EXTRACTION_DIRECTORY": str(self.extraction_directory)}

        self.processed_years: dict[str, bool] = dict()
        self.processed_years_record_file_path = self.records_directory / "processed_years.yaml"
        if self.processed_years_record_file_path.exists():
            with self.processed_years_record_file_path.open(mode="r") as file_stream:
                self.processed_years = yaml.safe_load(stream=file_stream)

        self.processed_months_per_year: dict[str, dict[str, bool]] = dict()
        self.processed_months_per_year_record_file_path = self.records_directory / "processed_months_per_year.yaml"
        if self.processed_months_per_year_record_file_path.exists():
            with self.processed_months_per_year_record_file_path.open(mode="r") as file_stream:
                self.processed_months_per_year = yaml.safe_load(stream=file_stream)

    def extract_s3_bucket(
        self,
        *,
        s3_root: str,
        limit: int | None = None,
        workers: int = -2,
        batch_size: int = 5_000,
        manifest_file_path: str | pathlib.Path | None = None,
    ) -> None:
        _handle_aws_credentials()
        max_workers = _handle_max_workers(workers=workers)

        unprocessed_s3_urls = self._get_unprocessed_s3_urls(manifest_file_path=manifest_file_path, s3_root=s3_root)
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
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                for batch in tqdm.tqdm(
                    iterable=batches,
                    total=len(batches),
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

        self._update_records()
        shutil.rmtree(path=self.temporary_directory, ignore_errors=True)

    def _get_unprocessed_s3_urls(self, manifest_file_path: pathlib.Path | None, s3_root: str) -> list[str]:
        self._get_end_record_and_check_consistency()

        self.processed_dates: dict[str, bool] = dict()
        processed_dates_record_file_path = self.records_directory / "processed_dates.yaml"
        if processed_dates_record_file_path.exists():
            with processed_dates_record_file_path.open(mode="r") as file_stream:
                self.processed_dates = yaml.safe_load(stream=file_stream)

        unprocessed_s3_urls_from_manifest = self._get_unprocessed_s3_urls_from_manifest(
            manifest_file_path=manifest_file_path, s3_root=s3_root
        )
        unprocessed_s3_urls_from_remote = self._get_unprocessed_s3_urls_from_remote(s3_root=s3_root)
        unprocessed_s3_urls = unprocessed_s3_urls_from_manifest + unprocessed_s3_urls_from_remote

        del self.s3_url_processing_end_record  # Free memory

        # Randomize the order of the remote files for the progress bar to be more accurate
        random.shuffle(x=unprocessed_s3_urls)

        return unprocessed_s3_urls

    def _get_end_record_and_check_consistency(self) -> None:
        self.s3_url_processing_end_record = dict()
        s3_url_processing_record_difference = set()
        if (
            self.s3_url_processing_start_record_file_path.exists()
            and self.s3_url_processing_end_record_file_path.exists()
        ):
            s3_url_processing_start_record = {
                file_path for file_path in self.s3_url_processing_start_record_file_path.read_text().splitlines()
            }
            self.s3_url_processing_end_record = {
                file_path: True for file_path in self.s3_url_processing_end_record_file_path.read_text().splitlines()
            }
            s3_url_processing_record_difference = s3_url_processing_start_record - set(
                self.s3_url_processing_end_record.keys()
            )
        if len(s3_url_processing_record_difference) > 0:
            # IDEA: an advanced feature for the future could be looking at the timestamp of the 'started' log
            # and cleaning the entire extraction directory of entries with that date (and possibly +/- a day around it)
            message = (
                "\nRecord corruption from previous run detected - "
                "please call `s3_log_extraction reset extraction` to clean the extraction cache and records.\n\n"
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
        unprocessed_dates = list(set(dates_from_manifest) - set(self.processed_dates.keys()))

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

        unprocessed_s3_urls = list(set(s3_urls) - set(self.s3_url_processing_end_record.keys()))
        return unprocessed_s3_urls

    def _get_unprocessed_s3_urls_from_remote(self, s3_root: str) -> list[str]:
        years_result = _deploy_subprocess(
            command=f"s5cmd ls {s3_root}/", error_message=f"Failed to scan years of nested structure at {s3_root}."
        )
        years = {line.split(" ")[-1].rstrip("/\n") for line in years_result.splitlines()}
        unprocessed_years = list(years - set(self.processed_years.keys()))

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
            unprocessed_months_per_year[year] = list(
                months - set(self.processed_months_per_year.get(year, dict()).keys())
            )

            for month in unprocessed_months_per_year[year]:
                subdirectory = f"{s3_root}/{year}/{month}"
                days_result = _deploy_subprocess(
                    command=f"s5cmd ls {subdirectory}/", error_message=f"Failed to list structure of {subdirectory}/."
                )
                if days_result is None:
                    continue

                dates = [f"{year}-{month}-{line.split(" ")[-1].rstrip("/\n")}" for line in days_result.splitlines()]
                dates_with_logs.extend(dates)

        new_dates = list(set(dates_with_logs) - set(self.processed_dates.keys()))
        sorted_new_dates = sorted(list(new_dates))
        unprocessed_dates = sorted_new_dates[:-2]  # Give a 2-day buffer to allow AWS to catch up

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
            s3_urls = [f"{subdirectory}/{line.split(" ")[-1].rstrip("\n")}" for line in s3_urls_result.splitlines()]

        unprocessed_s3_urls = list(set(s3_urls) - set(self.s3_url_processing_end_record.keys()))
        return unprocessed_s3_urls

    def _extract_s3_url(
        self,
        s3_url: str,
        enable_stop: bool = True,
        parallel_mode: bool = False,
    ) -> None:
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

        gawk_command = f"{self.gawk_base} --file {absolute_script_path} {absolute_file_path}"
        _deploy_subprocess(
            command=gawk_command,
            environment_variables=self._awk_env,
            error_message=f"Extraction failed on {file_path}.",
        )

    def _update_records(self) -> None:
        pass
        # TODO
        # for year, months in unprocessed_months_per_year.items():
        #     for month in months:
        #         processed_days_this_month = [
        #             processed_date
        #             for processed_date in processed_dates.keys()
        #             if processed_date.startswith(f"{year}-{month}-")
        #         ]
        #         total_days_this_month = calendar.monthrange(int(year), int(month))[1]
        #         if len(processed_days_this_month) == total_days_this_month:
        #             self.processed_months_per_year[year][month] = True
        #
        #     if len(self.processed_months_per_year.get(year, dict())) == 12:
        #         self.processed_years[year] = True
        #
        # with self.processed_months_per_year_record_file_path.open("w") as file_stream:
        #     yaml.dump(data=self.processed_months_per_year, stream=file_stream)
        # with self.processed_years_record_file_path.open("w") as file_stream:
        #     yaml.dump(data=self.processed_years, stream=file_stream)

    @staticmethod
    def parse_manifest(*, file_path: str | pathlib.Path) -> None:
        """
        Read the manifest file and save it as a parsed JSON object, adjacent to the initial file.

        The raw manifest file is the output of `s5cmd ls s3_root/* > manifest.txt`.
        """
        file_path = pathlib.Path(file_path)

        manifest = collections.defaultdict(list)
        lines = [line.split(" ")[-1].strip() for line in file_path.read_text().splitlines() if "DIR" not in line]
        for line in tqdm.tqdm(
            iterable=lines,
            total=len(lines),
            desc="Parsing local manifest",
            unit="files",
            smoothing=0,
            leave=False,
        ):
            line_splits = line.split("-")
            year = line_splits[0]
            month = line_splits[1]
            day = line_splits[2]
            date = f"{year}-{month}-{day}"
            manifest[date].append(line)

        parsed_file_path = file_path.parent / f"{file_path.stem}_parsed.json"
        parsed_file_path.unlink(missing_ok=True)
        with parsed_file_path.open(mode="w") as file_stream:
            json.dump(obj=dict(manifest), fp=file_stream)

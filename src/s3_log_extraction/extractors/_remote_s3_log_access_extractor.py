import calendar
import collections
import json
import pathlib
import shutil
import tempfile
import typing

import tqdm
import yaml

from ._utils import _deploy_subprocess, _handle_aws_credentials
from ..config import get_cache_directory, get_records_directory


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
        cache_directory = cache_directory or get_cache_directory()
        self.records_directory = get_records_directory(cache_directory=cache_directory)

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

        self.processed_dates: dict[str, bool] = dict()
        self.processed_dates_record_file_path = self.records_directory / "processed_dates.yaml"
        if self.processed_dates_record_file_path.exists():
            with self.processed_dates_record_file_path.open(mode="r") as file_stream:
                self.processed_dates = yaml.safe_load(stream=file_stream)

    def extract_s3(
        self,
        *,
        s3_url: str,
        date_limit: int | None = None,
        file_limit: int | None = None,
        workers: int = -2,
        mode: typing.Literal["dandi"] | None = None,
        manifest_file_path: str | pathlib.Path | None = None,
    ) -> None:
        """
        The protocol for iteratively downloading log files from a nested S3 structure.

        This is the preferred method of extracting S3 logs, as it allows for more efficient processing of large numbers
        of log files.

        Will skip the last two days which have logs available, assuming they are still being processed by AWS.
        """
        _handle_aws_credentials()

        manifest = dict()
        manifest_file_path = pathlib.Path(manifest_file_path) if manifest_file_path is not None else None
        if manifest_file_path is not None:
            with manifest_file_path.open(mode="r") as file_stream:
                manifest = json.load(fp=file_stream)

        dates_with_logs = [date for date in manifest.keys()]
        date_to_download_type = {date: "run" for date in manifest.keys()}

        years_result = _deploy_subprocess(
            command=f"s5cmd ls {s3_url}/", error_message=f"Failed to scan years of nested structure at {s3_url}."
        )
        years_from_nested = {line.split(" ")[-1].rstrip("/\n") for line in years_result.splitlines()}
        years_from_manifest = {f"{date.split("-")[0]}" for date in manifest.keys()}
        years = years_from_nested | years_from_manifest
        unprocessed_years = list(years - set(self.processed_years.keys()))

        unprocessed_months_per_year = dict()
        for year in tqdm.tqdm(
            iterable=unprocessed_years,
            total=len(unprocessed_years),
            desc="Assembling nested manifest",
            unit="years",
            smoothing=0,
            miniters=1,
            leave=False,
        ):
            months_result = _deploy_subprocess(command=f"s5cmd ls {s3_url}/{year}/", ignore_errors=True)
            if months_result is None:
                continue

            months_from_nested = {f"{line.split(" ")[-1].rstrip("/\n")}" for line in months_result.splitlines()}
            months_from_manifest = {f"{date.split('-')[1]}" for date in manifest.keys() if date.startswith(year)}
            months = months_from_nested | months_from_manifest
            unprocessed_months_per_year[year] = list(
                months - set(self.processed_months_per_year.get(year, dict()).keys())
            )

            for month in unprocessed_months_per_year[year]:
                days_result = _deploy_subprocess(command=f"s5cmd ls {s3_url}/{year}/{month}/", ignore_errors=True)
                if days_result is None:
                    continue

                dates_from_nested = {
                    f"{year}-{month}-{line.split(" ")[-1].rstrip("/\n")}" for line in days_result.splitlines()
                }
                dates_from_manifest = {date for date in manifest.keys() if date.startswith(f"{year}-{month}-")}
                dates = dates_from_nested | dates_from_manifest
                new_dates = list(dates - set(self.processed_dates.keys()))

                dates_with_logs.extend(new_dates)
                date_to_download_type.update({date: "cp" for date in new_dates})

        sorted_dates_with_logs = sorted(dates_with_logs)
        unprocessed_dates = sorted_dates_with_logs[:-2]  # Give a 2-day buffer to allow AWS to catch up
        dates_to_process = unprocessed_dates[:date_limit] if date_limit is not None else unprocessed_dates

        s3_base = s3_url.split("/")[2]
        temporary_directory = pathlib.Path(tempfile.mkdtemp(prefix="s3logextraction-"))
        for date in tqdm.tqdm(
            iterable=dates_to_process,
            total=len(dates_to_process),
            desc="Downloading and extracting S3 logs",
            unit="days",
            smoothing=0,
            miniters=1,
        ):
            date_directory = temporary_directory / date
            date_directory.mkdir(exist_ok=True)
            date_directory_string = date_directory.absolute().as_posix()

            year, month, day = date.split("-")
            s3_subdirectory = f"{s3_url}/{year}/{month}/{day}"

            if date_to_download_type[date] == "cp":
                s5cmd_cp_command = f"s5cmd cp {s3_subdirectory}/* {date_directory}"
            else:
                s5cmd_batch_file_path = date_directory / "s5cmd_batch.txt"
                s5cmd_batch_file_path.write_text(
                    "\n".join(
                        [
                            f"cp s3://{s3_base}/{filename} {date_directory_string}/{filename}"
                            for filename in manifest[date]
                        ]
                    )
                )
                s5cmd_cp_command = f"s5cmd run {s5cmd_batch_file_path}"
            _deploy_subprocess(
                command=s5cmd_cp_command,
                error_message=f"Failed to download days of nested structure at {s3_subdirectory}.",
            )

            # It might seem a tad silly to call our own CLI from here, but it is the most straightforward way
            # to leverage the more efficient file-wise parallelism
            limit_flag = f" --limit {file_limit}" if file_limit is not None else ""
            workers_flag = f" --workers {workers}" if workers != -2 else ""
            mode_flag = f" --mode {mode}" if mode is not None else ""
            s3logextraction_command = f"s3logextraction extract {date_directory}{limit_flag}{workers_flag}{mode_flag}"
            _deploy_subprocess(
                command=s3logextraction_command,
                error_message=f"Failed to extract content of S3 logs from {s3_subdirectory}.",
            )

            self.processed_dates[date] = True
            with self.processed_dates_record_file_path.open("a") as file_stream:
                file_stream.write(f'"{date}": True\n')

            shutil.rmtree(path=date_directory)
        shutil.rmtree(path=temporary_directory)

        # Update records
        for year, months in unprocessed_months_per_year.items():
            for month in months:
                processed_days_this_month = [
                    processed_date
                    for processed_date in self.processed_dates.keys()
                    if processed_date.startswith(f"{year}-{month}-")
                ]
                total_days_this_month = calendar.monthrange(int(year), int(month))[1]
                if len(processed_days_this_month) == total_days_this_month:
                    self.processed_months_per_year[year][month] = True

            if len(self.processed_months_per_year.get(year, dict())) == 12:
                self.processed_years[year] = True

        with self.processed_months_per_year_record_file_path.open("w") as file_stream:
            yaml.dump(data=self.processed_months_per_year, stream=file_stream)
        with self.processed_years_record_file_path.open("w") as file_stream:
            yaml.dump(data=self.processed_years, stream=file_stream)

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
            desc="Assembling additional manifest",
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

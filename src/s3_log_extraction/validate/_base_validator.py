import abc
import csv
import gzip
import hashlib
import json
import pathlib
import random
import tempfile

import tqdm

from ..config import get_records_directory


class BaseValidator(abc.ABC):
    """Base class for all log validators."""

    tqdm_description = "Validating log files"

    def __hash__(self) -> int:
        checksum = hashlib.sha1(string=self._run_validation.__code__.co_code).hexdigest()
        checksum_int = int(checksum, 16)
        return checksum_int

    def __init__(self) -> None:
        self.records_directory = get_records_directory()

        record_file_name = f"{self.__class__.__name__}_{hex(hash(self))[2:]}.txt"
        self.record_file_path = self.records_directory / record_file_name

        self.record: set[str] = set()
        if not self.record_file_path.exists():
            return

        with self.record_file_path.open(mode="r") as file_stream:
            self.record = {line.strip() for line in file_stream.readlines()}

    @abc.abstractmethod
    def _run_validation(self, file_path: pathlib.Path) -> None:
        """
        The rules by which the validation is performed on a single log file.

        Parameters
        ----------
        file_path : str
            The file path to validate.

        Raises
        ------
        ValueError or RuntimeError
            Any time the validation rule detects a violation.
        """
        message = "Validation rule has not been implemented for this class."
        raise NotImplementedError(message)

    def _record_success(self, file_path: pathlib.Path) -> None:
        """To avoid needlessly rerunning the validation process, we record the file path in a cache file."""
        with self.record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

    def validate_file(self, file_path: str | pathlib.Path) -> None:
        """
        Validate the log file according to the specified rule and if successful, record result in the cache.

        Parameters
        ----------
        file_path : path-like
            The file path to validate.
        """
        file_path = pathlib.Path(file_path)
        absolute_file_path = str(file_path.absolute())
        if absolute_file_path in self.record:
            return

        self._run_validation(file_path=file_path)

        self.record.add(absolute_file_path)
        self._record_success(file_path=file_path)

    def validate_directory(self, directory: str | pathlib.Path, limit: int | None = None) -> None:
        """
        Validate all log files in the specified directory according to the specified rule.

        Parameters
        ----------
        directory : path-like
            The directory to validate.
        limit : int, optional
            The maximum number of files to validate.
            If None, all files will be validated.
            The default is None.
        """
        directory = pathlib.Path(directory)

        all_log_files = {str(file_path.absolute()) for file_path in directory.rglob(pattern="*.log")}
        unvalidated_files = list(all_log_files - self.record)
        random.shuffle(unvalidated_files)

        files_to_validate = unvalidated_files[:limit] if limit is not None else unvalidated_files
        for file_path in tqdm.tqdm(
            iterable=files_to_validate,
            desc=self.tqdm_description,
            total=len(files_to_validate),
            unit="files",
            smoothing=0,
        ):
            self.validate_file(file_path=file_path)

    def validate_s3_bucket(
        self,
        *,
        s3_root: str,
        limit: int | None = None,
        inventory_directory: str | pathlib.Path | None = None,
    ) -> None:
        """
        Validate a random selection of S3 log files present in the inventory but not yet in this validator's record.

        Parameters
        ----------
        s3_root : str
            The root S3 path of the log bucket (e.g. ``s3://my-logs-bucket/logs``).
        limit : int or None, optional
            Maximum number of files to validate.  If ``None`` (default), all
            unvalidated files are validated.
        inventory_directory : path-like or None, optional
            Path to a local pre-downloaded S3 inventory directory.  The
            directory must follow the standard AWS S3 Inventory layout::

                <inventory_directory>/
                ├── <timestamp>/          # e.g. 2026-05-03T01-00Z/
                │   └── manifest.json
                ├── data/
                │   └── <uuid>.csv.gz
                └── hive/
                    └── dt=<YYYY-MM-DD-HH-MM>/
                        └── symlink.txt

            If not provided, an ``NotImplementedError`` is raised because remote
            listing without a local inventory is not yet supported.

        Raises
        ------
        NotImplementedError
            If ``inventory_directory`` is ``None`` (remote listing without a
            local inventory is not yet implemented).
        FileNotFoundError
            If no hive partitions are found in the inventory directory.
        ValueError
            If the ``Key`` column is absent from the inventory schema.
        """
        import fsspec

        if inventory_directory is not None:
            all_s3_urls = self._get_s3_urls_from_local_inventory(
                inventory_directory=pathlib.Path(inventory_directory),
                s3_root=s3_root,
            )
        else:
            message = (
                "Remote listing without a local inventory directory is not yet supported. "
                "Please provide an 'inventory_directory'."
            )
            raise NotImplementedError(message)

        unvalidated_urls = [url for url in all_s3_urls if url not in self.record]
        random.shuffle(unvalidated_urls)
        urls_to_validate = unvalidated_urls[:limit] if limit is not None else unvalidated_urls

        with tempfile.TemporaryDirectory(prefix="s3logextraction-validate-") as tmp_dir:
            tmp_path = pathlib.Path(tmp_dir)
            for s3_url in tqdm.tqdm(
                iterable=urls_to_validate,
                desc=self.tqdm_description,
                total=len(urls_to_validate),
                unit="files",
                smoothing=0,
            ):
                if s3_url in self.record:
                    continue

                filename = s3_url.split("/")[-1]
                temp_file = tmp_path / filename
                with fsspec.open(urlpath=s3_url, mode="rb") as remote_file:
                    temp_file.write_bytes(remote_file.read())

                try:
                    self._run_validation(file_path=temp_file)
                finally:
                    temp_file.unlink(missing_ok=True)

                self.record.add(s3_url)
                self._record_s3_url_success(s3_url=s3_url)

    def _get_s3_urls_from_local_inventory(
        self,
        inventory_directory: pathlib.Path,
        s3_root: str,
    ) -> list[str]:
        """
        Read all S3 URLs from a local AWS S3 Inventory directory that match the given ``s3_root``.

        Parameters
        ----------
        inventory_directory : pathlib.Path
            Path to the pre-downloaded S3 inventory root directory.
        s3_root : str
            The root S3 path prefix used to filter keys
            (e.g. ``s3://my-logs-bucket/logs``).

        Returns
        -------
        list[str]
            All matching S3 URLs found in the most recent inventory snapshot.

        Raises
        ------
        FileNotFoundError
            If no hive partitions are found or required files are missing.
        ValueError
            If the ``Key`` column is absent from the inventory schema.
        """
        # 1. Find the most recent hive partition (alphabetical sort works for dt=YYYY-MM-DD-HH-MM)
        hive_directory = inventory_directory / "hive"
        hive_partitions = sorted(hive_directory.glob("dt=*"))
        if not hive_partitions:
            message = f"No hive partitions found in {hive_directory}."
            raise FileNotFoundError(message)
        latest_partition = hive_partitions[-1]

        # 2. Derive the corresponding timestamp directory name.
        #    dt=2026-05-03-01-00  →  2026-05-03T01-00Z
        dt_value = latest_partition.name[len("dt=") :]
        date_part = dt_value[:10]
        time_part = dt_value[11:]
        timestamp_dir_name = f"{date_part}T{time_part}Z"
        manifest_path = inventory_directory / timestamp_dir_name / "manifest.json"

        with manifest_path.open(mode="r") as file_stream:
            manifest = json.load(fp=file_stream)

        source_bucket: str = manifest["sourceBucket"]
        file_schema = [col.strip() for col in manifest["fileSchema"].split(",")]
        if "Key" not in file_schema:
            message = f"'Key' column not found in inventory schema: {file_schema}"
            raise ValueError(message)
        key_index = file_schema.index("Key")

        # 3. Read symlink.txt — each line is an S3 path to a data/*.csv.gz file.
        symlink_path = latest_partition / "symlink.txt"
        symlink_lines = [line.strip() for line in symlink_path.read_text().splitlines() if line.strip()]

        # 4. Parse each local CSV.gz file referenced by the symlink.
        s3_root_prefix = s3_root.rstrip("/") + "/"
        s3_urls: list[str] = []
        for s3_data_path in symlink_lines:
            uuid_filename = s3_data_path.split("/")[-1]
            local_csv_gz_path = inventory_directory / "data" / uuid_filename
            with gzip.open(local_csv_gz_path, "rt", newline="") as gz_file:
                reader = csv.reader(gz_file)
                for row in reader:
                    if len(row) <= key_index:
                        continue
                    key = row[key_index]
                    s3_url = f"s3://{source_bucket}/{key}"
                    if not s3_url.startswith(s3_root_prefix):
                        continue
                    s3_urls.append(s3_url)

        return s3_urls

    def _record_s3_url_success(self, s3_url: str) -> None:
        """
        Record a successfully validated S3 URL in the validator's cache file.

        Parameters
        ----------
        s3_url : str
            The S3 URL that was successfully validated.
        """
        with self.record_file_path.open(mode="a") as file_stream:
            file_stream.write(f"{s3_url}\n")

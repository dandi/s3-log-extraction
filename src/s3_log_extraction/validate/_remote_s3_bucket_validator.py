import pathlib
import random

import tqdm

from .._utils import _read_s3_urls_from_local_inventory
from ..config import get_records_directory


class RemoteS3BucketValidator:
    """
    Validator that asserts the existence of S3 log files listed in a local AWS S3 Inventory.

    For each sampled URL that has not yet been recorded, the class checks whether the
    file is present on the S3 bucket.  URLs that are confirmed to exist are appended to
    this validator's record so that they are skipped on subsequent runs.

    This validator is:
      - not parallelized
      - interruptible
      - updatable
    """

    tqdm_description = "Validating S3 log file existence"

    def __init__(self, *, cache_directory: pathlib.Path | None = None) -> None:
        self.records_directory = get_records_directory(cache_directory=cache_directory)

        record_file_name = f"{self.__class__.__name__}.txt"
        self.record_file_path = self.records_directory / record_file_name

        self.record: set[str] = set()
        if not self.record_file_path.exists():
            return

        with self.record_file_path.open(mode="r") as file_stream:
            self.record = {line.strip() for line in file_stream.readlines() if line.strip()}

    def validate_s3_bucket(
        self,
        *,
        s3_root: str,
        limit: int | None = None,
        inventory_directory: str | pathlib.Path | None = None,
    ) -> None:
        """
        Assert that a random selection of S3 log files from the inventory exist on the bucket.

        For each sampled URL that has not yet been recorded, the method checks
        whether the file is present on the S3 bucket.  URLs that are confirmed
        to exist are appended to this validator's record.

        Parameters
        ----------
        s3_root : str
            The root S3 path of the log bucket (e.g. ``s3://my-logs-bucket/logs``).
        limit : int or None, optional
            Maximum number of files to check.  If ``None`` (default), all
            unvalidated files are checked.
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

            If not provided, a ``NotImplementedError`` is raised because remote
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

        s3_filesystem = fsspec.filesystem("s3")
        for s3_url in tqdm.tqdm(
            iterable=urls_to_validate,
            desc=self.tqdm_description,
            total=len(urls_to_validate),
            unit="files",
            smoothing=0,
        ):
            if not s3_filesystem.exists(s3_url):
                continue

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
        inventory = _read_s3_urls_from_local_inventory(
            inventory_directory=inventory_directory,
            s3_root=s3_root,
        )
        return [url for url_list in inventory.values() for url in url_list]

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

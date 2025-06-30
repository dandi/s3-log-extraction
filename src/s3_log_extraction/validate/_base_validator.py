import abc
import hashlib
import pathlib
import random

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

        self.record = {}
        if not self.record_file_path.exists():
            return

        with self.record_file_path.open(mode="r") as file_stream:
            self.record = {line.strip(): True for line in file_stream.readlines()}

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
        if self.record.get(absolute_file_path, False) is True:
            return

        self._run_validation(file_path=file_path)

        self.record[absolute_file_path] = True
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
        unvalidated_files = list(all_log_files - set(self.record.keys()))
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

import abc
import hashlib
import pathlib

from ..config import get_validation_directory


class BaseValidator:
    """Base class for all log validators."""

    def __init__(self) -> None:
        self.validation_directory = get_validation_directory()

        validation_rule_checksum = hashlib.sha1(string=self._run_validation.__code__.co_code).hexdigest()
        self.validator_record_file = self.validation_directory / f"{validation_rule_checksum}.txt"

        self.record = {}
        if not self.validator_record_file.exists():
            return

        with open(file=self.validator_record_file, mode="r") as file_stream:
            record_lines = file_stream.readlines()

        for line in record_lines:
            self.record[line] = True

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
        with open(file=self.validator_record_file, mode="a") as file_stream:
            file_stream.write(f"{file_path}\n")

    def validate_file(self, file_path: str | pathlib.Path) -> None:
        """
        Validate the log file according to the specified rule and if successful, record result in the cache.

        Parameters
        ----------
        file_path : path-like
            The file path to validate.
        """
        absolute_path = str(file_path.absolute())
        if self.record.get(absolute_path, False) is False:
            return
        file_path = pathlib.Path(file_path)

        self._run_validation(file_path=file_path)

        self.record[absolute_path] = True
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

        all_log_files = {str(file_path.absolute()) for file_path in directory.rglob("*.log")}
        unvalidated_files = all_log_files - set(self.record.keys())

        files_to_validate = list(unvalidated_files)[:limit] if limit is not None else unvalidated_files
        for file_path in files_to_validate:
            self.validate_file(file_path=file_path)

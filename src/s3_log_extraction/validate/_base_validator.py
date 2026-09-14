import abc
import hashlib
import pathlib
import random
import subprocess

import tqdm

from ..config import get_cache_subdirectory


def _hash_awk_script_file(script_path: pathlib.Path, /) -> int:
    """
    Compute a hash based on the contents of the AWK validation script.

    Editing the rule therefore starts a fresh validation record, since the record file of a validator is
    named after this value.

    Returns
    -------
    int
        Integer hash derived from the SHA-1 checksum of the AWK script file.
    """
    with script_path.open("rb") as file_stream:
        byte_content = file_stream.read()

    checksum = hashlib.sha1(string=byte_content).hexdigest()
    return int(checksum, 16)


def _run_awk_validation(
    *,
    script_path: pathlib.Path,
    file_path: pathlib.Path,
    failure_label: str,
    environment_variables: dict[str, str] | None = None,
) -> None:
    """
    Run a pre-validator's AWK script over a single log file, raising if the script reports a violation.

    Parameters
    ----------
    script_path : pathlib.Path
        The AWK script carrying the validation rule.
    file_path : pathlib.Path
        The raw S3 log file to validate.
    failure_label : str
        Leading phrase of the error message, naming the rule that failed.
    environment_variables : dict of str to str, optional
        Passed to the subprocess unchanged. The default of ``None`` lets the child inherit this process's
        environment, which is what every pre-validator but the extraction heuristic relies on. A mapping
        *replaces* that environment rather than adding to it, so it is deliberately never merged with
        ``os.environ``.

    Raises
    ------
    RuntimeError
        If the AWK script exits with a non-zero return code.
    """
    absolute_awk_script_path = str(script_path.absolute())
    absolute_file_path = str(file_path.absolute())

    awk_command = f"awk --file {absolute_awk_script_path} {absolute_file_path}"
    result = subprocess.run(
        args=awk_command,
        shell=True,
        capture_output=True,
        text=True,
        env=environment_variables,
    )
    if result.returncode != 0:
        message = (
            f"\n{failure_label} pre-check failed.\n "
            f"Log file: {absolute_file_path}\n"
            f"Error code {result.returncode}\n\n"
            f"stderr: {result.stderr}\n"
        )
        raise RuntimeError(message)


class BaseValidator(abc.ABC):
    """Base class for all log validators."""

    tqdm_description = "Validating log files"

    def __hash__(self) -> int:
        checksum = hashlib.sha1(string=self._run_validation.__code__.co_code).hexdigest()
        return int(checksum, 16)

    def __init__(self) -> None:
        self.records_directory = get_cache_subdirectory(name="records")

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

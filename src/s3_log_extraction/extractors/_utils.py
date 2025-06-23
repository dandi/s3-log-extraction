import os
import pathlib
import subprocess
import sys
import time
import warnings


def _deploy_subprocess(
    *,
    command: str | list[str],
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
) -> str | None:
    error_message = error_message or "An error occurred while executing the command."

    result = subprocess.run(
        args=command,
        env=environment_variables,
        shell=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0 and ignore_errors is False:
        message = (
            f"\n\nError code {result.returncode}\n"
            f"{error_message}\n\n"
            f"stdout: {result.stdout}\n\n"
            f"stderr: {result.stderr}\n\n"
        )
        raise RuntimeError(message)
    if result.returncode != 0 and ignore_errors is True:
        return None

    return result.stdout


def _append_with_lock(file_path: pathlib.Path, content: str, retries: int = 240, delay: float = 1) -> None:
    system = sys.platform
    match system:
        case "win32":
            import msvcrt

            for attempt in range(retries):
                try:
                    with file_path.open(mode="a") as file_stream:
                        file_descriptor = file_stream.fileno()
                        nbytes = file_path.stat().st_size

                        try:
                            msvcrt.locking(file_descriptor, msvcrt.LK_NBLCK, nbytes)
                            file_stream.write(content)
                            return
                        finally:
                            msvcrt.locking(file_descriptor, msvcrt.LK_UNLCK, nbytes)
                except PermissionError:
                    if attempt >= retries - 1:
                        raise
                    time.sleep(delay)
        case _:
            import fcntl

            with file_path.open(mode="a") as file_stream:
                file_descriptor = file_stream.fileno()

                for attempt in range(retries):
                    try:
                        fcntl.flock(file_descriptor, fcntl.LOCK_EX)
                        file_stream.write(content)
                        return
                    except (BlockingIOError, OSError):
                        if attempt >= retries - 1:
                            raise
                        time.sleep(delay)
                    finally:
                        fcntl.flock(file_descriptor, fcntl.LOCK_NB)


def _handle_max_workers(*, workers: int) -> int:
    """
    Handle the number of workers for parallel processing.

    If workers is 0, it raises a warning and sets it to -2 (default).
    If workers is negative, it calculates the maximum number of workers based on CPU count.
    If workers is positive, it ensures it does not exceed the CPU count.
    """
    if workers == 0:
        message = "The number of workers cannot be 0 - please set it to an integer. Falling back to default of -2."
        warnings.warn(message=message, stacklevel=2)
        workers = -2

    if workers != 1 and sys.platform == "win32":
        message = "Parallelism is not supported on Windows - forcing the number of workers to 1."
        warnings.warn(message=message, stacklevel=2)
        workers = 1

    cpu_count = os.cpu_count()
    if workers < 0:
        max_workers = workers % cpu_count + 1
    elif workers > cpu_count:
        max_workers = cpu_count
    else:
        max_workers = workers

    return max_workers


def _handle_aws_credentials() -> None:
    """Handle AWS credentials by checking environment variables or the AWS credentials file."""
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID", None)
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY", None)
    aws_credentials_file_path = pathlib.Path.home() / ".aws" / "credentials"

    if aws_access_key_id is None or aws_secret_access_key is None and aws_credentials_file_path.exists():
        with aws_credentials_file_path.open(mode="r") as file_stream:
            aws_credentials_content = file_stream.read()
        if (
            aws_credentials_content.count("aws_access_key_id") > 1
            or aws_credentials_content.count("aws_secret_access_key") > 1
        ):
            message = (
                "Missing environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` and multiple AWS "
                "credentials were found in the system credentials file - please set the environment variables "
                "to disambiguate."
            )
            raise ValueError(message)
        aws_access_key_id = next(line.strip() for line in aws_credentials_content.splitlines())
        aws_secret_access_key = next(
            line.strip() for line in aws_credentials_content.splitlines() if "aws_secret_access_key" in line
        )

    if aws_access_key_id is None or aws_secret_access_key is None:
        message = (
            "Missing environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` - "
            "please set your these variables or configure via AWS CLI."
        )
        raise ValueError(message)

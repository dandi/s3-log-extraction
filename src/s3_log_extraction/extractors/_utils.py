import os
import pathlib
import subprocess

from ..ip_utils._ip_utils import _read_ips_from_file, _write_ips_to_file


def _merge_file_into_extraction(
    *,
    source_file_path: pathlib.Path,
    destination_file_path: pathlib.Path,
    use_encryption: bool,
) -> None:
    """Merge a single `.txt` file from `source_file_path` into `destination_file_path`.

    For ``ips.txt`` files, the existing encrypted destination is decrypted, merged with the new
    plaintext IPs, and re-encrypted. All other files are appended as raw bytes.
    """
    if use_encryption and source_file_path.name == "ips.txt":
        new_ips = _read_ips_from_file(file_path=source_file_path, use_encryption=False)
        existing_ips = (
            _read_ips_from_file(file_path=destination_file_path, use_encryption=True)
            if destination_file_path.exists()
            else []
        )
        _write_ips_to_file(
            file_path=destination_file_path,
            ips=[*existing_ips, *new_ips],
            use_encryption=True,
        )
    else:
        content = source_file_path.read_bytes()
        with destination_file_path.open(mode="ab") as file_stream:
            file_stream.write(content)


def _merge_dir_to_extraction(
    *,
    source_dir: pathlib.Path,
    extraction_directory: pathlib.Path,
    use_encryption: bool,
) -> None:
    """Merge all `.txt` files from `source_dir` into `extraction_directory`.

    For ``ips.txt`` files, existing encrypted content is decrypted, merged with new plaintext
    IPs, and re-encrypted. All other files are appended as raw bytes.
    """
    for file_path in source_dir.rglob(pattern="*.txt"):
        relative_parts = file_path.relative_to(source_dir).parts
        destination_file_path = extraction_directory / pathlib.Path(*relative_parts)
        destination_file_path.parent.mkdir(parents=True, exist_ok=True)
        _merge_file_into_extraction(
            source_file_path=file_path,
            destination_file_path=destination_file_path,
            use_encryption=use_encryption,
        )


def _deploy_subprocess(
    *,
    command: str,
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
) -> str | None:
    error_message = error_message or "An error occurred while executing the command."

    # Merge custom environment variables with current environment
    # This preserves key variables such as PATH
    env = os.environ.copy()
    if environment_variables is not None:
        env.update(environment_variables)

    result = subprocess.run(
        args=command,
        env=env,
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

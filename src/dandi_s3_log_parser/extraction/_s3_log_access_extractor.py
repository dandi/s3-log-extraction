import os
import pathlib
import shutil
import subprocess

from dandi_s3_log_parser.config._config import get_cache_directory


class S3LogAccessExtractor:
    def __init__(self, log_directory: str | pathlib.Path) -> None:
        """
        An extractor of basic access information contained in raw S3 logs.

        This class is not a full parser of all fields but instead is optimized for targeting the most relevant
        information for the DANDI Archive. The information is then reduced to a smaller storage size for further
        processing.

        Parameters
        ----------
        log_directory : path-like
            The directory containing the raw S3 log files to be processed.

            Must follow the structure:

            <directory>
                ├── YYYY
                │   ├── MM
                │   │   ├── DD.log
                │   │   └── ...
                │   └── ...
                └── ...
        """
        self.log_directory = pathlib.Path(log_directory)

        self.cache_directory = get_cache_directory()

        self.extraction_directory = self.cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)
        self.blobs_directory = self.extraction_directory / "blobs"
        self.blobs_directory.mkdir(exist_ok=True)
        self.zarr_directory = self.extraction_directory / "zarr"
        self.zarr_directory.mkdir(exist_ok=True)

        # TODO: might have to be done inside subfunction used by other parallel processes
        self.temporary_directory = self.cache_directory / "tmp" / os.getgid()
        self.temporary_directory.parent.mkdir(exist_ok=True)
        self.temporary_directory.mkdir(exist_ok=True)

        self.record_directory = self.cache_directory / "extraction_records"
        self.record_directory.mkdir(exist_ok=True)

        # NOTE: the script below is not true 'parsing'
        # If it fails on a given line no error is raised or tracked and the line is effectively skipped
        # We proceed therefore under the assumption that this rule works only on well-formed lines
        # And that any line it fails on would also have an error status code as a result
        # TODO: needs a verification procedure to ensure
        self.timestamp_file_path = self.temporary_directory / "timestamps.txt"
        self.ip_file_path = self.temporary_directory / "ips.txt"
        self.object_key_file_path = self.temporary_directory / "blobs.txt"
        self.bytes_file_path = self.temporary_directory / "bytes.txt"
        self.extraction_awk_script = (
            "awk -F'\" '{"
            '    split($1, pre_uri_fields, " ");'
            '    split($3, post_uri_fields, " ");'
            ""
            "    timestamp = pre_uri_fields[3];"
            "    ip = pre_uri_fields[5];"
            "    request_type = pre_uri_fields[8];"
            "    object_key = pre_uri_fields[9];"
            "    status = post_uri_fields[1];"
            "    bytes_sent = post_uri_fields[3];"
            ""
            '    if (request_type == "REST.GET.OBJECT" & substr(status, 1, 1) == "2") {'
            f'       print timestamp > "{self.timestamp_file_path}";'
            f'       print ip > "{self.ip_file_path}";'
            f'       print object_key > "{self.object_key_file_path}";'
            f'       print bytes_sent > "{self.bytes_file_path}";'
            "    }"
            # f"}} {log_file_path}"
        )

    def extract(self) -> None:
        count = 0
        for log_file_path in self.log_directory.rglob("*.log"):
            self._extract_log(log_file_path=log_file_path)
            count += 1

            if count > 3:
                break

    def _extract_log(self, log_file_path: pathlib.Path) -> None:
        year = log_file_path.parent.parent.name
        month = log_file_path.parent.name
        record_file_path = self.record_directory / year / month / log_file_path.name
        record_file_path.parent.mkdir(parents=True, exist_ok=True)

        # TODO: parallelize
        result = subprocess.run(args=self.awk_script, shell=True, capture_output=True)

        if result.returncode != 0:
            message = (
                f"Failed to extract log file {log_file_path} with:\n"
                f"Error code {result.returncode}\n"
                f"stderr: {result.stderr}"
            )
            raise RuntimeError(message)

        # This part not parallelized
        # with open(file=self.timestamp_file_path, mode="r") as file_stream:
        #     timestamps = file_stream.readlines()
        # with open(file=self.ip_file_path, mode="r") as file_stream:
        #     ips = file_stream.readlines()
        # with open(file=self.object_key_file_path, mode="rb") as file_stream:
        #     object_keys = [self._sanitize_object_key(object_key=object_key) for object_key in file_stream.readlines()]
        # with open(file=self.bytes_file_path, mode="r") as file_stream:
        #     bytes_sent = file_stream.readlines()

        # unique_object_keys = set(object_keys) - {""}
        # for object_key in unique_object_keys:
        #     extraction_file_path

        record_file_path.touch()
        shutil.rmtree(self.temporary_directory)

    @staticmethod
    def _sanitize_object_key(object_key_bytes: bytes) -> str | None:
        """
        Sanitize the raw bytes for the object key to remove any unwanted parts.

        Parameters
        ----------
        object_key : bytes
            The object key to be sanitized, in bytes.

        Returns
        -------
        str or None
            The sanitized object key, decoded as a string.
            Returns None if the object key is neither 'zarr' nor 'blobs'.
        """
        if object_key_bytes[:4] == b"zarr":
            object_key = object_key_bytes[:41].decode("utf-8")
            return object_key
        elif object_key_bytes[:5] == b"blobs":
            object_key = (object_key_bytes[:6] + object_key_bytes[14:]).decode("utf-8")
            return object_key

        return None

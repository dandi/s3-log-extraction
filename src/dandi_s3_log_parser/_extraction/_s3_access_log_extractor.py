import os
import subprocess

from .._config import get_cache_directory

class S3AccessLogExtractor:
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

        self.extraction_directory = cache_directory / "extraction"
        self.extraction_directory.mkdir(exist_ok=True)

        self.temporary_directory = cache_directory / "tmp" / os.getgid()
        self.temporary_directory.parent.mkdir(exist_ok=True)
        self.temporary_directory.mkdir(exist_ok=True)
        self.temporary_blobs_directory = self.temporary_directory / "blobs"
        self.temporary_blobs_directory.mkdir(exist_ok=True)
        self.temporary_zarr_directory = self.temporary_directory / "zarr"
        self.temporary_zarr_directory.mkdir(exist_ok=True)

        self.record_directory = self.cache_directory / "extraction_records"
        self.record_directory.mkdir(exist_ok=True)

        # NOTE: the script below is not true 'parsing'
        # If it fails on a given line no error is raised or tracked and the line is effectively skipped
        # We proceed therefore under the assumption that this rule works only on well-formed lines
        # And that any line it fails on would also have an error status code as a result
        # TODO: needs a verification procedure to ensure
        self.timestamp_file_path = self.temporary_directory / "timestamps.txt"
        self.ip_file_path = self.temporary_directory / "ips.txt"
        self.blob_file_path = self.temporary_directory / "blobs.txt"
        self.bytes_file_path = self.temporary_directory / "bytes.txt"
        self.extraction_awk_script = (
            "awk -F'\" '{"
            '    split($1, pre_uri_fields, " ");'
            '    split($3, post_uri_fields, " ");'
            ''
            '    timestamp = pre_uri_fields[3];'
            '    ip = pre_uri_fields[5];'
            '    request_type = pre_uri_fields[8];'
            '    blob = pre_uri_fields[9];'
            '    status = post_uri_fields[1];'
            '    bytes_sent = post_uri_fields[3];'
            ''
            '    if (request_type == "REST.GET.OBJECT" & substr(status, 1, 1) == "2") {'
            f'       print timestamp > "{self.timestamp_file_path}";'
            f'       print ip > "{self.ip_file_path}";'
            f'       print blob > "{self.blob_file_path}";'
            f'       print bytes_sent > "{self.bytes_file_path}";'
            '    }'
            f"}} {log_file_path}"
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
        subprocess.run(args=self.awk_script, shell=True)

        record_file_path.touch()



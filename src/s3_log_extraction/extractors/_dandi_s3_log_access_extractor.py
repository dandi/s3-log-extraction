import collections
import pathlib

from ._s3_log_access_extractor import S3LogAccessExtractor
from .._regex import DROGON_IP_REGEX_ENCRYPTED
from ..encryption_utils import decrypt_bytes


class DandiS3LogAccessExtractor(S3LogAccessExtractor):
    """
    A DANDI-specific extractor of basic access information contained in raw S3 logs.

    This class is not a full parser of all fields but instead is optimized for targeting the most relevant
    information for reporting summaries of access.

    The `extraction` subdirectory within the cache directory will contain a mirror of the object structures
    from the S3 bucket; except Zarr stores, which are abbreviated to their top-most level.

    This extractor is:
      - parallelized
      - semi-interruptible; most of the computation via AWK can be interrupted safely, but not the mirror copy step
      - updatable

    Parameters
    ----------
    log_directory : path-like
        The directory containing the raw S3 log files to be processed.
    """

    def __init__(self, cache_directory: pathlib.Path | None = None) -> None:
        super().__init__(cache_directory=cache_directory)

        self._relative_script_path = pathlib.Path(__file__).parent / "_fast_dandi_extraction.awk"

        ips_to_skip_regex = decrypt_bytes(encrypted_data=DROGON_IP_REGEX_ENCRYPTED)
        self._awk_env["IPS_TO_SKIP_REGEX"] = ips_to_skip_regex.decode("utf-8")

    # def _run_extraction(self, *, file_path: pathlib.Path) -> None:
    #     absolute_script_path = str(self._relative_script_path.absolute())
    #     absolute_file_path = str(file_path.absolute())
    #
    #     absolute_temporary_directory = str(self.temporary_directory.absolute()) + str(pathlib.Path("/"))
    #     self._awk_env["TEMPORARY_DIRECTORY"] = absolute_temporary_directory
    #
    #     awk_command = f"{self.awk_base} --file {absolute_script_path} {absolute_file_path}"
    #     result = subprocess.run(
    #         args=awk_command,
    #         shell=True,
    #         capture_output=True,
    #         text=True,
    #         env=self._awk_env,
    #     )
    #     if result.returncode != 0:
    #         message = (
    #             f"\nExtraction failed.\n "
    #             f"Log file: {absolute_file_path}\n"
    #             f"Error code {result.returncode}\n\n"
    #             f"stderr: {result.stderr}\n"
    #         )
    #         raise RuntimeError(message)

    # def _bin_and_save_extracted_data(
    #     self,
    #     *,
    #     object_keys: typing.Iterable[str],
    #     all_data: typing.Iterable[str | int],
    #     filename: str,
    #     write_format: typing.Literal["%d", "%s"],
    # ) -> None:
    #     data_per_object_key = collections.defaultdict(list)
    #     for object_key, data in zip(object_keys, all_data):
    #         data_per_object_key[object_key].append(data)
    #
    #     for object_key, data in data_per_object_key.items():
    #         mirror_directory = self.extraction_directory / object_key
    #         mirror_file_path = mirror_directory / filename
    #         with mirror_file_path.open(mode="a") as file_stream:
    #             numpy.savetxt(fname=file_stream, X=data, fmt=write_format)

    def _mirror_copy(self) -> None:
        # Mirror the timestamps
        # object_keys = numpy.loadtxt(fname=self.object_keys_file_path, dtype=str)
        all_object_keys = self.object_keys_file_path.read_text().splitlines()

        # unique_object_keys = numpy.unique(object_keys)
        unique_object_keys = {object_key for object_key in all_object_keys}
        # for object_key in unique_object_keys:
        #     mirror_directory = self.extraction_directory / object_key
        #     mirror_directory.mkdir(parents=True, exist_ok=True)
        collections.deque(
            (
                (self.extraction_directory / object_key).mkdir(parents=True, exist_ok=True)
                for object_key in unique_object_keys
            ),
            maxlen=0,
        )
        del unique_object_keys  # Clear memory to reduce overhead

        # all_timestamps = numpy.loadtxt(fname=self.timestamps_file_path, dtype="uint64")
        # all_timestamps = self.timestamps_file_path.read_text().splitlines()
        # self._bin_and_save_extracted_data(
        #     object_keys=all_object_keys,
        #     all_data=all_timestamps,
        #     filename="timestamps.txt",
        #     write_format="%d",
        # )
        # del all_timestamps

        # all_bytes_sent = numpy.loadtxt(fname=self.bytes_sent_file_path, dtype="uint64")
        # all_bytes_sent = self.bytes_sent_file_path.read_text().splitlines()
        # self._bin_and_save_extracted_data(
        #     object_keys=all_object_keys,
        #     all_data=all_bytes_sent,
        #     filename="bytes_sent.txt",
        #     write_format="%d",
        # )
        # del all_bytes_sent

        # all_ips = numpy.loadtxt(fname=self.ips_file_path, dtype="U15")
        # all_ips = self.ips_file_path.read_text().splitlines()
        # self._bin_and_save_extracted_data(
        #     object_keys=all_object_keys,
        #     all_data=all_ips,
        #     filename="full_ips.txt",
        #     write_format="%s",
        # )
        # del all_ips

        # data_per_object_key = collections.defaultdict(list)
        # for object_key, data in zip(object_keys, all_data):
        #     data_per_object_key[object_key].append(data)
        # data_per_object_key = {
        #     object_key: [data for object_key, data in zip(all_object_keys, all_ips)]
        # }

        # data_per_object_key = collections.defaultdict(lambda: ([], [], []))
        # for object_key, timestamp, bytes_sent, ip in zip(all_object_keys, all_timestamps, all_bytes_sent, all_ips):
        #     data_per_object_key[object_key][0].append(f"{timestamp}\n")
        #     data_per_object_key[object_key][1].append(f"{bytes_sent}\n")
        #     data_per_object_key[object_key][2].append(f"{ip}\n")

        with (
            self.timestamps_file_path.open(mode="r") as timestamps_file,
            self.bytes_sent_file_path.open(mode="r") as bytes_sent_file,
            self.ips_file_path.open(mode="r") as ips_file,
        ):
            data_per_object_key = collections.defaultdict(lambda: ([], [], []))
            for object_key, timestamp, bytes_sent, ip in zip(
                all_object_keys, timestamps_file, bytes_sent_file, ips_file
            ):
                data_per_object_key[object_key][0].append(timestamp)
                data_per_object_key[object_key][1].append(bytes_sent)
                data_per_object_key[object_key][2].append(ip)

        for object_key, data in data_per_object_key.items():
            mirror_directory = self.extraction_directory / object_key
            with (
                (mirror_directory / "timestamps.txt").open(mode="a") as ts_file,
                (mirror_directory / "bytes_sent.txt").open(mode="a") as bs_file,
                (mirror_directory / "full_ips.txt").open(mode="a") as ip_file,
            ):
                ts_file.writelines(data[0])
                bs_file.writelines(data[1])
                ip_file.writelines(data[2])

    # def extract_file(self, file_path: str | pathlib.Path) -> None:
    #     pid = str(os.getpid())
    #     if self.stop_file_path.exists() is True:
    #         print(f"Extraction stopped on process {pid} - exiting...")
    #         return
    #
    #     file_path = pathlib.Path(file_path)
    #     absolute_file_path = str(file_path.absolute())
    #     if self.extraction_record.get(absolute_file_path, False) is True:
    #         return
    #
    #     self.temporary_directory = self.base_temporary_directory / pid
    #     self.temporary_directory.mkdir(exist_ok=True)
    #     self.object_keys_file_path = self.temporary_directory / "object_keys.txt"
    #     self.timestamps_file_path = self.temporary_directory / "timestamps.txt"
    #     self.bytes_sent_file_path = self.temporary_directory / "bytes_sent.txt"
    #     self.ips_file_path = self.temporary_directory / "full_ips.txt"
    #
    #     self._run_extraction(file_path=file_path)
    #
    #     # Sometimes a log file (especially very early ones) may not have any valid GET entries
    #     if not self.object_keys_file_path.exists():
    #         return
    #
    #     # Record the start of the mirror copy step
    #     with self.mirror_copy_start_record_file_path.open(mode="a") as file_stream:
    #         file_stream.write(f"{absolute_file_path}\n")
    #
    #     self._mirror_copy()
    #
    #     # Record final success and cleanup
    #     with self.mirror_copy_end_record_file_path.open(mode="a") as file_stream:
    #         file_stream.write(f"{absolute_file_path}\n")
    #     shutil.rmtree(path=self.temporary_directory)
    #
    #     self.extraction_record[absolute_file_path] = True
    #     with self.extraction_record_file_path.open(mode="a") as file_stream:
    #         file_stream.write(f"{absolute_file_path}\n")

import itertools
import pathlib
import random
import shutil
import typing
import warnings

import tqdm

_DEFAULT_FIELDS = (
    "8787a3c41bf7ce0d54359d9348ad5b08e16bd5bb8ae5aa4e1508b435773a066e",
    "dandiarchive",
    "[01/Jan/2020:05:06:35",
    "+0000]",
    "192.0.2.0",
    "-",
    "J42N2W7ET0EC03CV",
    "REST.GET.OBJECT",
    "ds006260/sub-Re19/ses-1/eeg/sub-Re19_ses-1_task-SmartickDataset_run-1_channels.tsv",
    '"GET',
    "/ds006260/sub-Re19/ses-1/eeg/sub-Re19_ses-1_task-SmartickDataset_run-1_channels.tsv",
    'HTTP/1.1"',
    "206",
    "-",
    "384",
    "384",
    "53",
    "52",
    '"-"',
    '"-"',
    "-",
    "DX8oFoKQx0o5V3lwEuWBxF5p2fSXrwINj0rnxmas0YgjWuPqYLK/vnW60Txh23K93aahe0IFw2c=",
    "-",
    "ECDHE-RSA-AES128-GCM-SHA256",
    "-",
    "dandiarchive.s3.amazonaws.com",
    "TLSv1.2",
    "-",
)
_MONTH_TO_STR = {
    "01": "Jan",
    "02": "Feb",
    "03": "Mar",
    "04": "Apr",
    "05": "May",
    "06": "Jun",
    "07": "Jul",
    "08": "Aug",
    "09": "Sep",
    "10": "Oct",
    "11": "Nov",
    "12": "Dec",
}
# There are of course many more request and status types, but limiting for simplicity
_POSSIBLE_REQUEST_TYPES = ("REST.GET.OBJECT", "REST.PUT.OBJECT", "REST.DELETE.OBJECT", "REST.HEAD.OBJECT")
_POSSIBLE_STATUS_CODES = ("200", "206", "304", "400", "403", "404")


def generate_benchmark(directory: str | pathlib.Path, seed: int = 0) -> None:
    """
    Generate a ~120 MB directory of random log files for benchmarking the S3 log extraction tools.

    This does not exhaustively span the extremes of log contents as seen from the tests, but rather replicates
    some of the easier lines with randomized variation in some fields.

    The resulting log files may not actually be strictly valid S3 logs as the request type is sometimes changed
    independently of other fields that may be incompatible.

    Parameters
    ----------
    directory : str | pathlib.Path
        Path to the directory where the benchmarks will be generated.
    seed : int
        Seed for the random number generator to ensure reproducibility.
        Default is 0.
    """
    directory = pathlib.Path(directory)
    random.seed(a=seed)

    object_key_levels = tuple(_generate_object_key_levels(number_of_object_key_levels=(4, 3, 2)))
    object_keys = tuple(_generate_object_keys(number_of_object_keys=40, levels=object_key_levels))

    # In reality this is a much more complicated multi-modal distribution
    object_key_to_total_bytes = {object_key: random.randint(a=4096, b=100_000_000_000) for object_key in object_keys}

    benchmark_directory = directory / "s3-log-extraction-benchmark"
    if benchmark_directory.exists() and any(benchmark_directory.iterdir()):
        message = f"\n\nDirectory {benchmark_directory} is not empty. Existing contents will be removed.\n\n"
        warnings.warn(message=message, stacklevel=2)
        shutil.rmtree(path=benchmark_directory)
    benchmark_directory.mkdir(exist_ok=True)

    _create_date_directories(directory=benchmark_directory)
    _create_random_log_files(directory=benchmark_directory, object_key_to_total_bytes=object_key_to_total_bytes)


def _generate_object_key_levels(
    *, number_of_object_key_levels: tuple[int, ...], length: int = 6, characters: str = "0123456789abcdef"
) -> typing.Generator[str]:
    """
    Generate a random combination of levels (subdirectories relative to root) for S3 object keys.

    Parameters
    ----------
    number_of_object_key_levels : tuple[int, ...]
        A tuple where each element specifies the number of levels to generate for that level of the object key.
        For example, (20, 10, 5) means 20 outer levels, 10 second levels per outer level, and so on.
        This will therefore generate 20*10*5 = 1000 total combinations of object key levels.
    length : int
        The length of each random string to generate for the object key levels.
        Default is 6.
        If this is desired to be varied as well, please raise an issue to request.
    characters : str
        The characters to use for generating the random strings.
        Default is "0123456789abcdef", which is hexadecimal.

    Returns
    -------
    object_key_levels : typing.Generator[str]
        A generator that yields random combinations of object key levels as strings.
        Each string is a combination of the specified number of levels, joined by slashes ("/").
    """
    levels = [
        ["".join(random.choices(population=characters, k=length)) for _ in range(number_of_levels)]
        for number_of_levels in number_of_object_key_levels
    ]
    for combo in itertools.product(*levels):
        yield "/".join(combo)


def _generate_object_keys(
    *,
    number_of_object_keys: int,
    levels: tuple[str, ...],
    lower_bound: int = 5,
    higher_bound: int = 30,
    characters: str = "0123456789abcdef",
) -> typing.Generator[str]:
    """
    Generate random S3 object keys based on the provided levels.

    These typically represent files, but suffixes are not currently added.

    Parameters
    ----------
    number_of_object_keys : int
        The number of object keys to generate.
    levels : tuple[str, ...]
        A tuple of strings representing the levels (subdirectories) for the object keys.
    lower_bound : int
        The minimum length of the object key.
        Default is 5.
    higher_bound : int
        The maximum length of the object key.
        Default is 30.
    characters : str
        The characters to use for generating the random strings.
        Default is "0123456789abcdef", which is hexadecimal.

    Returns
    -------
    object_keys : typing.Generator[str]
        A generator that yields random S3 object keys as strings.
    """
    for _ in range(number_of_object_keys):
        level = random.choice(seq=levels)
        length = random.randint(lower_bound, higher_bound)
        yield f"{level}/{''.join(random.choices(population=characters, k=length))}"


def _create_date_directories(
    *,
    directory: pathlib.Path,
    start_year: int = 2019,
    end_year: int = 2024,
) -> None:
    """
    Create year/month/day directories for the specified range of years.

    Parameters
    ----------
    directory : pathlib.Path
        The base directory where the year/month/day directories will be created.
    start_year : int
        The starting year for the directory structure.
    end_year : int
        The ending year for the directory structure.
    """
    for year in range(start_year, end_year + 1):
        year_directory = directory / str(year)
        year_directory.mkdir(exist_ok=True)

        for month in range(1, 13):
            month_directory = year_directory / f"{month:02d}"
            month_directory.mkdir(exist_ok=True)

            for day in range(1, 29):  # Not including 29th, 30th and 31st for simplicity
                day_directory = month_directory / f"{day:02d}"
                day_directory.mkdir(exist_ok=True)


def _create_random_log_files(
    *,
    directory: pathlib.Path,
    object_key_to_total_bytes: dict[str, int],
    number_of_files_per_day_lower_bound: int = 1,
    number_of_files_per_day_upper_bound: int = 20,
) -> None:
    # These are frozen since they are mandated by the S3 filename format
    characters: str = "0123456789ABCDEF"
    length: int = 16

    # For efficiency, only cast this once
    object_keys = list(object_key_to_total_bytes.keys())

    pre_path_to_number_of_files_per_day = {
        f"{year.name}/{month.name}/{day.name}/{year.name}-{month.name}-{day.name}": random.randint(
            a=number_of_files_per_day_lower_bound, b=number_of_files_per_day_upper_bound
        )
        for year in directory.iterdir()
        for month in year.iterdir()
        for day in month.iterdir()
    }
    for pre_path, number_of_files_per_day in tqdm.tqdm(
        iterable=pre_path_to_number_of_files_per_day.items(),
        total=len(pre_path_to_number_of_files_per_day),
        desc="Generating benchmark files",
        unit="files",
    ):
        for file_index in range(number_of_files_per_day):
            # NOTE: This can technically result in collisions at a low probability
            hour = random.randint(a=0, b=23)
            minute = random.randint(a=0, b=59)
            second = random.randint(a=0, b=59)
            random_id = "".join(random.choices(population=characters, k=length))
            subpath = f"{pre_path}-{hour}-{minute}-{second}-{random_id}"
            file_path = directory / subpath
            _create_random_log_file(
                file_path=file_path,
                object_keys=object_keys,
                object_key_to_total_bytes=object_key_to_total_bytes,
            )


def _create_random_log_file(
    file_path: pathlib.Path,
    object_keys: list[str],
    object_key_to_total_bytes: dict[str, int],
    lines_per_file_lower_bound: int = 2,
    lines_per_file_upper_bound: int = 15,
) -> None:
    number_of_lines = random.randint(a=lines_per_file_lower_bound, b=lines_per_file_upper_bound)
    timestamp = "-".join(file_path.name.split("-")[:-1])
    lines = _generate_random_lines(
        number_of_lines=number_of_lines,
        timestamp=timestamp,
        object_keys=object_keys,
        object_key_to_total_bytes=object_key_to_total_bytes,
    )
    with file_path.open("w") as file_stream:
        file_stream.write("\n".join(lines))


def _generate_random_lines(
    number_of_lines: int, timestamp: str, object_keys: list[str], object_key_to_total_bytes: dict[str, int]
) -> typing.Generator[str]:
    for _ in range(number_of_lines):
        line = list(_DEFAULT_FIELDS)

        year, month, day, hour, minute, second = timestamp.split("-")
        line[2] = f"[{day:02s}/{_MONTH_TO_STR[month]}/{year}:{hour:02s}:{minute:02s}:{second:02s}"

        request_type = random.choice(seq=_POSSIBLE_REQUEST_TYPES)
        line[7] = request_type

        # In reality, there are clear correlations between objects keys and request types over nearby periods of time
        object_key = random.choice(seq=object_keys)
        line[8] = object_key
        line[10] = f"/{object_key}"

        status_code = random.choice(seq=_POSSIBLE_STATUS_CODES)
        line[12] = status_code

        # In reality, this value may not even be present for certain request or status code combinations
        total_bytes = object_key_to_total_bytes[object_key]
        bytes_sent = random.randint(a=1, b=total_bytes) if random.random() >= 0.01 else "-"
        line[14] = str(bytes_sent)
        line[15] = str(total_bytes)

        yield " ".join(line)

import csv
import gzip
import io
import json
import pathlib

import pytest
import yaml

import s3_log_extraction
from s3_log_extraction.ip_utils import MappingRegionResolver
from s3_log_extraction.utils import read_text_from_file

_MOCKED_IP_TO_REGION_FILE_PATH = pathlib.Path(__file__).parent / "mocked_ips" / "ip_to_region.yaml"


def pytest_configure(config: pytest.Config) -> None:
    """
    Fail any test that meets a published service range listing which cannot be obtained.

    The tools only warn about it at run time. Failing on it here is what lets the daily remote tests report a source
    that has broken, rather than pass with its ranges missing. The filter is added here rather than under
    ``filterwarnings`` in ``pyproject.toml``, because resolving the warning category there imports the package before
    coverage starts, which leaves every module-level line uncovered.
    """
    config.addinivalue_line("filterwarnings", "error::s3_log_extraction.ip_utils.ServiceNetworksWarning")


@pytest.fixture(autouse=True)
def default_listings_cache_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path_factory: pytest.TempPathFactory
) -> pathlib.Path:
    """
    Keep the copies of the published IP range listings that a fetch saves out of the developer's own cache directory.

    A fetch given no cache directory saves its copies, and reads them back, under the returned directory instead of
    the configured one. A copy left there by a real run would otherwise stand in for a listing a test breaks on
    purpose. A directory of its own is used rather than ``tmp_path``, so that tests inspecting their ``tmp_path`` do
    not find it there.
    """
    default_cache_directory = tmp_path_factory.mktemp("default_cache_directory")
    get_cache_subdirectory = s3_log_extraction.ip_utils._ip_utils.get_cache_subdirectory

    def _get_cache_subdirectory(*, cache_directory: str | pathlib.Path | None = None, name: str) -> pathlib.Path:
        return get_cache_subdirectory(
            cache_directory=cache_directory if cache_directory is not None else default_cache_directory, name=name
        )

    monkeypatch.setattr(s3_log_extraction.ip_utils._ip_utils, "get_cache_subdirectory", _get_cache_subdirectory)
    return default_cache_directory


@pytest.fixture
def mocked_region_resolver() -> MappingRegionResolver:
    """
    A stand-in geolocation of the requesters in the example logs.

    Every requester of the example logs is a documentation-range address (RFC 5737), which a real resolution labels
    ``bogon``; this resolver maps them to the invented regions of ``mocked_ips/ip_to_region.yaml`` instead, so that
    the summaries have resolved regions to report.
    """
    return MappingRegionResolver(yaml.safe_load(_MOCKED_IP_TO_REGION_FILE_PATH.read_text()))


@pytest.fixture
def use_mocked_region_resolver(
    monkeypatch: pytest.MonkeyPatch, mocked_region_resolver: MappingRegionResolver
) -> MappingRegionResolver:
    """Make the summaries resolve requesters with the mocked resolver, for tests that reach them through the CLI."""
    monkeypatch.setattr(
        "s3_log_extraction.summarize._generate_summaries.IpRegionResolver", lambda **kwargs: mocked_region_resolver
    )
    return mocked_region_resolver


def read_extracted_ips(extraction_directory: pathlib.Path, *, use_encryption: bool) -> list[str]:
    """Collect every IP address recorded across the `ips.txt` files of an extraction directory."""
    return [
        stripped
        for file_path in sorted(extraction_directory.rglob(pattern="ips.txt"))
        for line in read_text_from_file(file_path=file_path, use_encryption=use_encryption).splitlines()
        if (stripped := line.strip())
    ]


def write_by_region_summary(summary_file_path: pathlib.Path, regions: list[str]) -> None:
    """Write a minimal published by-region summary listing the given region labels."""
    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    rows = "\n".join(f"{region}\t1\t1\t0\t1" for region in regions)
    summary_file_path.write_text(
        f"region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\tnumber_of_views\n{rows}\n"
    )


def build_inventory_directory(
    tmp_path: pathlib.Path,
    *,
    source_bucket: str,
    rows: list[tuple],
    file_schema: str,
    timestamp: str = "2024-01-05T01-00Z",
    dt_partition: str = "dt=2024-01-05-01-00",
) -> pathlib.Path:
    """
    Create a minimal local AWS S3 Inventory directory for testing.

    Writes a single ``data/*.csv.gz`` holding *rows*, a ``hive/<dt_partition>/symlink.txt`` referencing it,
    and a ``<timestamp>/manifest.json`` recording the schema.

    Parameters
    ----------
    tmp_path : pathlib.Path
        Root directory under which the inventory tree is created.
    source_bucket : str
        Value placed in the ``sourceBucket`` field of ``manifest.json``.
    rows : list[tuple]
        Rows to write into the CSV data file; each tuple must match *file_schema*.
    file_schema : str
        Comma-separated column names written into ``manifest.json``.
    timestamp : str
        Name of the timestamped manifest directory, e.g. ``"2024-01-05T01-00Z"``.
    dt_partition : str
        Name of the hive partition directory, e.g. ``"dt=2024-01-05-01-00"``.

    Returns
    -------
    pathlib.Path
        The root of the created inventory directory.
    """
    inventory_dir = tmp_path / "inventory"
    inventory_dir.mkdir(exist_ok=True)
    data_dir = inventory_dir / "data"
    data_dir.mkdir(exist_ok=True)
    hive_dir = inventory_dir / "hive"
    hive_dir.mkdir(exist_ok=True)

    # Write CSV.gz data file
    csv_buffer = io.BytesIO()
    with gzip.open(csv_buffer, "wt", newline="") as gz:
        writer = csv.writer(gz)
        for row in rows:
            writer.writerow(row)
    uuid_filename = "test-uuid.csv.gz"
    (data_dir / uuid_filename).write_bytes(csv_buffer.getvalue())

    # Write manifest.json in the timestamp directory
    timestamp_dir = inventory_dir / timestamp
    timestamp_dir.mkdir(exist_ok=True)
    manifest = {
        "sourceBucket": source_bucket,
        "fileFormat": "CSV",
        "fileSchema": file_schema,
        "files": [
            {
                "key": f"inventory/{source_bucket}/data/{uuid_filename}",
                "size": len(csv_buffer.getvalue()),
            }
        ],
    }
    with (timestamp_dir / "manifest.json").open("w") as f:
        json.dump(manifest, f)

    # Write hive partition and symlink.txt
    partition_dir = hive_dir / dt_partition
    partition_dir.mkdir(exist_ok=True)
    s3_data_ref = f"s3://inventory-bucket/inventory/{source_bucket}/data/{uuid_filename}"
    (partition_dir / "symlink.txt").write_text(s3_data_ref + "\n")

    return inventory_dir


def build_key_inventory_directory(
    tmp_path: pathlib.Path,
    *,
    source_bucket: str,
    keys: list[str],
    timestamp: str = "2024-01-05T01-00Z",
    dt_partition: str = "dt=2024-01-05-01-00",
    file_schema: str = "Bucket, Key",
) -> pathlib.Path:
    """
    Create a minimal inventory directory whose every row is ``(source_bucket, key)``.

    The common special case of ``build_inventory_directory``, for tests that only care about object keys.
    """
    return build_inventory_directory(
        tmp_path,
        source_bucket=source_bucket,
        rows=[(source_bucket, key) for key in keys],
        file_schema=file_schema,
        timestamp=timestamp,
        dt_partition=dt_partition,
    )

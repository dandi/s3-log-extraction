import json
import pathlib
import secrets
import shutil

import pandas
import py
import pytest

import s3_log_extraction

# A view is a maximal run of streaming requests from one IP to one asset separated by no more than 8 hours.
# The timestamps below are all on 2025-01-01 unless noted, written in the extraction cache's YYMMDDHHmmss format.
_STREAMING = 0
_DOWNLOAD = 1


def _write_asset(
    *,
    asset_directory: pathlib.Path,
    requests: list[tuple[str, int, str]],
    use_encryption: bool = False,
) -> None:
    """
    Write the line-aligned per-request files of a single synthetic asset.

    Each entry of ``requests`` is a ``(timestamp, download, ip)`` triplet, where ``timestamp`` uses the
    ``YYMMDDHHmmss`` format of the extraction cache and ``download`` is ``1`` for a full download or
    ``0`` for a streaming request.
    """
    asset_directory.mkdir(parents=True, exist_ok=True)

    timestamps = [timestamp for timestamp, _, _ in requests]
    downloads = [str(download) for _, download, _ in requests]
    ips = [ip for _, _, ip in requests]

    (asset_directory / "timestamps.txt").write_text("\n".join(timestamps) + "\n")
    (asset_directory / "download.txt").write_text("\n".join(downloads) + "\n")
    (asset_directory / "bytes_sent.txt").write_text("\n".join(["1"] * len(requests)) + "\n")
    s3_log_extraction.utils.write_text_to_file(
        file_path=asset_directory / "ips.txt", text="\n".join(ips) + "\n", use_encryption=use_encryption
    )


def test_generic_summaries(tmpdir: py.path.local):
    test_dir = pathlib.Path(tmpdir)

    base_tests_dir = pathlib.Path(__file__).parent
    expected_output_dir = base_tests_dir / "expected_output"
    expected_extraction_dir = expected_output_dir / "extraction"
    expected_summaries_dir = expected_output_dir / "summaries"

    test_extraction_dir = test_dir / "extraction"
    test_summary_dir = test_dir / "summaries"
    shutil.copytree(src=expected_extraction_dir, dst=test_extraction_dir)

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir, use_encryption=False)
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    test_file_paths = {path.relative_to(test_summary_dir): path for path in test_summary_dir.rglob(pattern="*.tsv")}
    expected_file_paths = {
        path.relative_to(expected_summaries_dir): path for path in expected_summaries_dir.rglob(pattern="*.tsv")
    }
    assert set(test_file_paths.keys()) == set(expected_file_paths.keys())

    for expected_file_path in expected_file_paths.values():
        relative_file_path = expected_file_path.relative_to(expected_summaries_dir)
        test_file_path = test_summary_dir / relative_file_path

        test_mapped_log = pandas.read_table(filepath_or_buffer=test_file_path, index_col=0)
        expected_mapped_log = pandas.read_table(filepath_or_buffer=expected_file_path, index_col=0)

        # Pandas assertion makes no reference to the case being tested when it fails
        try:
            pandas.testing.assert_frame_equal(left=test_mapped_log, right=expected_mapped_log)
        except AssertionError as exception:
            message = (
                f"\n\nTest file path: {test_file_path}\nExpected file path: {expected_file_path}\n\n"
                f"{str(exception)}\n\n"
            )
            raise AssertionError(message)

    # Verify the single-value requester_count.tsv and view_count.tsv files
    single_value_file_names = ("requester_count.tsv", "view_count.tsv")
    test_tsv_paths = {
        path.relative_to(test_summary_dir): path
        for file_name in single_value_file_names
        for path in test_summary_dir.rglob(pattern=file_name)
    }
    expected_tsv_paths = {
        path.relative_to(expected_summaries_dir): path
        for file_name in single_value_file_names
        for path in expected_summaries_dir.rglob(pattern=file_name)
    }
    assert set(test_tsv_paths.keys()) == set(expected_tsv_paths.keys())

    for relative_path, expected_txt_path in expected_tsv_paths.items():
        test_txt_path = test_summary_dir / relative_path
        assert test_txt_path.read_text().strip() == expected_txt_path.read_text().strip(), (
            f"\n\nMismatch in {relative_path}:\n"
            f"  test:     {test_txt_path.read_text().strip()!r}\n"
            f"  expected: {expected_txt_path.read_text().strip()!r}\n"
        )

    # Verify totals.json
    test_totals = json.loads((test_summary_dir / "totals.json").read_text())
    expected_totals = json.loads((expected_summaries_dir / "totals.json").read_text())
    assert (
        test_totals == expected_totals
    ), f"\n\ntotals.json mismatch:\n  test:     {test_totals}\n  expected: {expected_totals}\n"

    # Verify archive_totals.json
    test_archive_totals = json.loads((test_summary_dir / "archive_totals.json").read_text())
    expected_archive_totals = json.loads((expected_summaries_dir / "archive_totals.json").read_text())
    assert (
        test_archive_totals == expected_archive_totals
    ), f"\n\narchive_totals.json mismatch:\n  test:     {test_archive_totals}\n  expected: {expected_archive_totals}\n"


def test_generate_all_dataset_totals_skips_archive(tmpdir: py.path.local):
    """Verify that the 'archive' subdirectory is excluded from dataset totals."""
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    # Set up a real dataset summary
    dataset_dir = summary_dir / "ds001161"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t1194564\t4\t3\n"
    )

    # Set up an archive summary that should be excluded
    archive_dir = summary_dir / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t7481053\t7\t5\n"
    )

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)

    totals = json.loads((summary_dir / "totals.json").read_text())
    assert "ds001161" in totals, "'ds001161' should be present in totals.json"
    assert "archive" not in totals, "'archive' should be excluded from totals.json"


@pytest.mark.ai_generated
def test_generate_archive_totals_raises_without_archive_requester_count(tmpdir: py.path.local) -> None:
    """Archive totals should fail if archive requester count has not been generated."""
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t7481053\t7\t5\n"
    )

    with pytest.raises(FileNotFoundError, match="Archive requester count file not found"):
        s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "downloads", "minimum_threshold", "expected_requests", "expected_downloads"),
    [
        (4, 14, 5, "<5", 20),
        (33, 36, 5, 40, 40),
    ],
)
def test_generate_archive_totals_thresholds_request_and_download_counts(
    tmpdir: py.path.local,
    requests: int,
    downloads: int,
    minimum_threshold: int,
    expected_requests: str | int,
    expected_downloads: str | int,
) -> None:
    test_dir = pathlib.Path(tmpdir)
    archive_dir = test_dir / "summaries" / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" f"missing\t10\t{requests}\t{downloads}\n"
    )
    (archive_dir / "requester_count.tsv").write_text("100\n")

    s3_log_extraction.summarize.generate_archive_totals(
        cache_directory=test_dir, privacy_threshold_minimum=minimum_threshold
    )

    archive_totals = json.loads((test_dir / "summaries" / "archive_totals.json").read_text())
    assert archive_totals["total_number_of_requests"] == expected_requests
    assert archive_totals["total_number_of_downloads"] == expected_downloads


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "downloads", "minimum_threshold", "expected_requests", "expected_downloads"),
    [
        (4, 14, 5, "<5", 20),
        (33, 36, 5, 40, 40),
    ],
)
def test_generate_all_dataset_totals_thresholds_request_and_download_counts(
    tmpdir: py.path.local,
    requests: int,
    downloads: int,
    minimum_threshold: int,
    expected_requests: str | int,
    expected_downloads: str | int,
) -> None:
    test_dir = pathlib.Path(tmpdir)
    dataset_dir = test_dir / "summaries" / "ds001"
    dataset_dir.mkdir(parents=True)
    (dataset_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n" f"missing\t10\t{requests}\t{downloads}\n"
    )
    (dataset_dir / "requester_count.tsv").write_text("100\n")

    s3_log_extraction.summarize.generate_all_dataset_totals(
        cache_directory=test_dir, privacy_threshold_minimum=minimum_threshold
    )

    totals = json.loads((test_dir / "summaries" / "totals.json").read_text())
    assert totals["ds001"]["total_number_of_requests"] == expected_requests
    assert totals["ds001"]["total_number_of_downloads"] == expected_downloads


@pytest.mark.ai_generated
def test_generate_archive_summaries_thresholds_request_and_download_columns(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir, privacy_threshold_minimum=4)

    archive_by_day = pandas.read_table(filepath_or_buffer=summary_dir / "archive" / "by_day.tsv")
    archive_by_region = pandas.read_table(filepath_or_buffer=summary_dir / "archive" / "by_region.tsv")
    assert archive_by_day.loc[0, "number_of_requests"] == "<4"
    assert archive_by_day.loc[0, "number_of_downloads"] == "<4"
    assert archive_by_region.loc[0, "number_of_requests"] == "<4"
    assert archive_by_region.loc[0, "number_of_downloads"] == "<4"


@pytest.mark.ai_generated
def test_generate_archive_summaries_aggregates_requester_count(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("60\n")

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("40\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_requester_count_file_path = summary_dir / "archive" / "requester_count.tsv"
    assert archive_requester_count_file_path.exists()
    assert archive_requester_count_file_path.read_text().strip() == "100"


@pytest.mark.ai_generated
def test_generate_archive_summaries_aggregates_optional_by_asset_type_per_week(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n2026-01-05\t3\t4\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n2026-01-05\t7\n")

    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    assert archive_file_path.exists()
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    expected_summary = pandas.DataFrame(
        data={
            "week_start": ["2025-12-29", "2026-01-05"],
            "Miscellaneous": [2, 4],
            "Neurophysiology": [1, 3],
            "Video": [5, 7],
        }
    )
    pandas.testing.assert_frame_equal(left=archive_summary, right=expected_summary)


@pytest.mark.ai_generated
def test_generate_archive_summaries_accepts_custom_asset_type_order(tmpdir: py.path.local) -> None:
    test_dir = pathlib.Path(tmpdir)
    summary_dir = test_dir / "summaries"

    ds001_dir = summary_dir / "ds001"
    ds001_dir.mkdir(parents=True)
    (ds001_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t10\t1\t1\n"
    )
    (ds001_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t1\t1\n"
    )
    (ds001_dir / "requester_count.tsv").write_text("20\n")
    (ds001_dir / "by_asset_type_per_week.tsv").write_text(
        "week_start\tNeurophysiology\tMiscellaneous\n2025-12-29\t1\t2\n"
    )

    ds002_dir = summary_dir / "ds002"
    ds002_dir.mkdir(parents=True)
    (ds002_dir / "by_day.tsv").write_text(
        "date\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\n2026-01-01\t40\t2\t1\n"
    )
    (ds002_dir / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t40\t2\t1\n"
    )
    (ds002_dir / "requester_count.tsv").write_text("20\n")
    (ds002_dir / "by_asset_type_per_week.tsv").write_text("week_start\tVideo\n2025-12-29\t5\n")

    s3_log_extraction.summarize.generate_archive_summaries(
        cache_directory=test_dir, asset_types_in_order=["Video", "Neurophysiology", "Miscellaneous"]
    )

    archive_file_path = summary_dir / "archive" / "by_asset_type_per_week.tsv"
    archive_summary = pandas.read_table(filepath_or_buffer=archive_file_path)
    assert archive_summary.columns.tolist() == ["week_start", "Video", "Neurophysiology", "Miscellaneous"]


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("count", "modulo", "minimum", "expected"),
    [
        # Below minimum → sentinel string
        (0, 20, 50, "<50"),
        (1, 20, 50, "<50"),
        (49, 20, 50, "<50"),
        # At or above minimum → rounded to nearest multiple of modulo
        (50, 20, 50, 40),  # round(2.5)=2 (banker's rounding)
        (55, 20, 50, 60),  # round(2.75)=3
        (60, 20, 50, 60),
        (100, 20, 50, 100),
        (123, 20, 50, 120),
        # Custom modulo and minimum
        (4, 5, 5, "<5"),
        (5, 5, 5, 5),
        (7, 5, 5, 5),
        (8, 5, 5, 10),
        # minimum can differ from modulo
        (9, 10, 5, 10),
        (3, 10, 5, "<5"),
    ],
)
def test_round_requester_count(count: int, modulo: int, minimum: int, expected: str | int):
    """Privacy-rounding returns the sentinel below minimum and rounds to the nearest modulo otherwise."""
    from s3_log_extraction.summarize._generate_summaries import _round_requester_count

    assert _round_requester_count(count=count, modulo=modulo, minimum=minimum) == expected


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("region_label", "expected"),
    [
        # Genuine known cloud service / VPN labels are excluded
        ("GitHub", True),
        ("VPN", True),
        ("AWS/us-east-1", True),
        ("GCP/us-central1", True),
        # Unresolved-location labels are NOT cloud/VPN labels; a real requester's IP
        # simply failed to geolocate (rate limit, quota exceeded, obscure IP, etc.)
        ("unknown", False),
        ("undetermined", False),
        ("missing", False),
        ("bogon", False),
        # Genuine geographic labels are not excluded
        ("US/California", False),
    ],
)
def test_is_cloud_service_or_vpn_label(region_label: str, expected: bool) -> None:
    """Only genuine cloud/VPN service labels are excluded; unresolved-location labels are not."""
    from s3_log_extraction.ip_utils import is_cloud_service_or_vpn_label

    assert is_cloud_service_or_vpn_label(region_label) == expected


@pytest.mark.ai_generated
def test_collect_unique_ips_excludes_known_cloud_service_ips(tmpdir: py.path.local) -> None:
    """Known cloud service/VPN IPs are excluded from the requester count, but unresolved-location IPs are not."""
    from s3_log_extraction.summarize._generate_summaries import _collect_unique_ips

    asset_dir = pathlib.Path(tmpdir) / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)
    (asset_dir / "ips.txt").write_text("1.2.3.4\n5.6.7.8\n9.10.11.12\n13.14.15.16\n17.18.19.20\n21.22.23.24\n")

    ip_to_region = {
        "1.2.3.4": "US/California",
        "5.6.7.8": "GitHub",
        "9.10.11.12": "AWS/us-east-1",
        "13.14.15.16": "VPN",
        "17.18.19.20": "bogon",
        "21.22.23.24": "unknown",
    }

    unique_ips = _collect_unique_ips(asset_directories=[asset_dir], use_encryption=False, ip_to_region=ip_to_region)

    assert unique_ips == {"1.2.3.4", "17.18.19.20", "21.22.23.24"}


@pytest.mark.ai_generated
def test_summarize_dataset_requester_count_excludes_known_cloud_service_ips(tmpdir: py.path.local) -> None:
    """The rounded requester count written to disk excludes known cloud service/VPN IPs."""
    from s3_log_extraction.summarize._generate_summaries import _summarize_dataset_requester_count

    asset_dir = pathlib.Path(tmpdir) / "asset"
    asset_dir.mkdir(parents=True, exist_ok=True)
    real_ips = [f"10.0.0.{index}" for index in range(60)]
    cloud_ips = [f"192.168.0.{index}" for index in range(60)]
    (asset_dir / "ips.txt").write_text("\n".join(real_ips + cloud_ips))

    ip_to_region = {ip: "US/California" for ip in real_ips} | {ip: "GitHub" for ip in cloud_ips}

    summary_file_path = pathlib.Path(tmpdir) / "requester_count.tsv"
    _summarize_dataset_requester_count(
        asset_directories=[asset_dir],
        summary_file_path=summary_file_path,
        ip_to_region=ip_to_region,
        minimum=50,
        use_encryption=False,
    )

    assert summary_file_path.read_text().strip() == "60"


@pytest.mark.ai_generated
@pytest.mark.parametrize(
    ("requests", "expected_number_of_views"),
    [
        # A single streaming request is a single view
        ([("250101000000", _STREAMING, "192.0.2.0")], 1),
        # A burst of streaming requests, as one exploration of a file emits, collapses into a single view
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101000001", _STREAMING, "192.0.2.0"),
                ("250101000135", _STREAMING, "192.0.2.0"),
            ],
            1,
        ),
        # A gap just under 8 hours does not cross the session boundary
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101075900", _STREAMING, "192.0.2.0")],
            1,
        ),
        # A gap of exactly 8 hours is not more than the timeout, so it does not split the session
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101080000", _STREAMING, "192.0.2.0")],
            1,
        ),
        # One second beyond 8 hours does split the session
        (
            [("250101000000", _STREAMING, "192.0.2.0"), ("250101080001", _STREAMING, "192.0.2.0")],
            2,
        ),
        # Timestamps are sorted before gaps are measured, so file order does not matter
        (
            [("250101080001", _STREAMING, "192.0.2.0"), ("250101000000", _STREAMING, "192.0.2.0")],
            2,
        ),
        # Three consecutive days of streaming are three views
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250102000000", _STREAMING, "192.0.2.0"),
                ("250103000000", _STREAMING, "192.0.2.0"),
            ],
            3,
        ),
        # Full downloads are not views
        (
            [("250101000000", _DOWNLOAD, "192.0.2.0"), ("250101120000", _DOWNLOAD, "192.0.2.0")],
            0,
        ),
        # A full download in the middle of a long gap does not bridge two streaming sessions
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101040000", _DOWNLOAD, "192.0.2.0"),
                ("250101090000", _STREAMING, "192.0.2.0"),
            ],
            2,
        ),
        # Each IP is sessionized independently, so interleaved requests do not merge
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101040000", _STREAMING, "198.51.100.0"),
                ("250101090000", _STREAMING, "192.0.2.0"),
                ("250101093000", _STREAMING, "198.51.100.0"),
            ],
            3,
        ),
        # A gap that would split one IP's session does not split another's
        (
            [
                ("250101000000", _STREAMING, "192.0.2.0"),
                ("250101000000", _STREAMING, "198.51.100.0"),
                ("250102000000", _STREAMING, "192.0.2.0"),
            ],
            3,
        ),
    ],
)
def test_count_asset_views(
    tmpdir: py.path.local, requests: list[tuple[str, int, str]], expected_number_of_views: int
) -> None:
    """Streaming sessions are counted per IP, split only by gaps of more than 8 hours."""
    from s3_log_extraction.summarize._generate_summaries import _count_asset_views

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(asset_directory=asset_directory, requests=requests)

    number_of_views = _count_asset_views(asset_directory=asset_directory, use_encryption=False)

    assert number_of_views == expected_number_of_views


@pytest.mark.ai_generated
def test_count_asset_views_respects_custom_session_timeout(tmpdir: py.path.local) -> None:
    """The session timeout is configurable, and the default of 8 hours is applied when it is not overridden."""
    from s3_log_extraction.summarize._generate_summaries import _count_asset_views
    from s3_log_extraction.summarize._globals import SESSION_TIMEOUT_SECONDS

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(
        asset_directory=asset_directory,
        requests=[("250101000000", _STREAMING, "192.0.2.0"), ("250101040000", _STREAMING, "192.0.2.0")],
    )

    assert SESSION_TIMEOUT_SECONDS == 28_800
    assert _count_asset_views(asset_directory=asset_directory, use_encryption=False) == 1
    assert _count_asset_views(asset_directory=asset_directory, use_encryption=False, session_timeout_seconds=3_600) == 2


@pytest.mark.ai_generated
def test_count_asset_views_warns_on_misaligned_files(tmpdir: py.path.local) -> None:
    """Per-asset files that are not line-aligned cannot be sessionized, so they are skipped with a warning."""
    from s3_log_extraction.summarize._generate_summaries import _count_asset_views

    asset_directory = pathlib.Path(tmpdir) / "asset"
    _write_asset(
        asset_directory=asset_directory,
        requests=[("250101000000", _STREAMING, "192.0.2.0"), ("250101000001", _STREAMING, "192.0.2.0")],
    )
    (asset_directory / "download.txt").write_text("0\n")

    with pytest.warns(UserWarning, match="mismatched line counts"):
        number_of_views = _count_asset_views(asset_directory=asset_directory, use_encryption=False)

    assert number_of_views == 0


@pytest.mark.ai_generated
def test_summaries_privacy_round_number_of_views(tmpdir: py.path.local) -> None:
    """Per-asset view counts are privacy-rounded exactly like the request and download counts."""
    test_dir = pathlib.Path(tmpdir)
    dataset_directory = test_dir / "extraction" / "ds001"

    # One view from each of sixty distinct requesters, which survives the disclosure threshold
    _write_asset(
        asset_directory=dataset_directory / "popular.nwb",
        requests=[(f"2501010000{index:02d}", _STREAMING, f"192.0.2.{index}") for index in range(60)],
    )
    # Three views from a single requester, which is censored
    _write_asset(
        asset_directory=dataset_directory / "quiet.nwb",
        requests=[
            ("250101000000", _STREAMING, "198.51.100.0"),
            ("250102000000", _STREAMING, "198.51.100.0"),
            ("250103000000", _STREAMING, "198.51.100.0"),
        ],
    )

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir, use_encryption=False)

    by_asset = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_asset.tsv", index_col=0)
    assert by_asset.columns.tolist() == ["bytes_sent", "number_of_requests", "number_of_downloads", "number_of_views"]
    assert by_asset.loc["popular.nwb", "number_of_views"] == "60"
    assert by_asset.loc["quiet.nwb", "number_of_views"] == "<50"


@pytest.mark.ai_generated
def test_summaries_count_views_with_encryption(tmpdir: py.path.local, monkeypatch: pytest.MonkeyPatch) -> None:
    """Views are counted from encrypted IP files, which is how the extraction cache stores them by default."""
    monkeypatch.setenv("S3_LOG_EXTRACTION_PASSWORD", secrets.token_urlsafe(32))

    test_dir = pathlib.Path(tmpdir)
    asset_directory = test_dir / "extraction" / "ds001" / "encrypted.nwb"

    # Two views for each of thirty requesters, split by a gap of more than 8 hours
    requests = []
    for index in range(30):
        ip = f"192.0.2.{index}"
        requests.extend(
            [
                (f"2501010000{index:02d}", _STREAMING, ip),
                (f"2501011200{index:02d}", _STREAMING, ip),
                (f"2501011200{index:02d}", _DOWNLOAD, ip),
            ]
        )
    _write_asset(asset_directory=asset_directory, requests=requests, use_encryption=True)

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir, use_encryption=True)

    by_asset = pandas.read_table(filepath_or_buffer=test_dir / "summaries" / "ds001" / "by_asset.tsv", index_col=0)
    assert by_asset.loc["encrypted.nwb", "number_of_views"] == 60
    assert (test_dir / "summaries" / "ds001" / "view_count.tsv").read_text().strip() == "60"


@pytest.mark.ai_generated
def test_view_counts_roll_up_to_dataset_and_archive_totals(tmpdir: py.path.local) -> None:
    """View counts sum across the assets of a dataset and across the datasets of the archive."""
    test_dir = pathlib.Path(tmpdir)
    extraction_directory = test_dir / "extraction"

    # ds001: forty views on one asset and twenty on another, for a dataset total of sixty
    _write_asset(
        asset_directory=extraction_directory / "ds001" / "first.nwb",
        requests=[(f"2501010000{index:02d}", _STREAMING, f"192.0.2.{index}") for index in range(40)],
    )
    _write_asset(
        asset_directory=extraction_directory / "ds001" / "second.nwb",
        requests=[(f"2501010000{index:02d}", _STREAMING, f"198.51.100.{index}") for index in range(20)],
    )
    # ds002: eighty views on a single asset
    _write_asset(
        asset_directory=extraction_directory / "ds002" / "third.nwb",
        requests=[
            (f"250101{index // 60:02d}{index % 60:02d}00", _STREAMING, f"192.0.2.{index}") for index in range(80)
        ],
    )

    s3_log_extraction.summarize.generate_summaries(cache_directory=test_dir, use_encryption=False)
    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_summaries(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    summary_directory = test_dir / "summaries"
    assert (summary_directory / "ds001" / "view_count.tsv").read_text().strip() == "60"
    assert (summary_directory / "ds002" / "view_count.tsv").read_text().strip() == "80"
    assert (summary_directory / "archive" / "view_count.tsv").read_text().strip() == "140"

    totals = json.loads((summary_directory / "totals.json").read_text())
    assert totals["ds001"]["total_number_of_views"] == 60
    assert totals["ds002"]["total_number_of_views"] == 80

    archive_totals = json.loads((summary_directory / "archive_totals.json").read_text())
    assert archive_totals["total_number_of_views"] == 140


@pytest.mark.ai_generated
def test_totals_report_censored_views_when_view_counts_are_missing(tmpdir: py.path.local) -> None:
    """A cache summarized before views existed reports the censored sentinel rather than failing."""
    test_dir = pathlib.Path(tmpdir)
    summary_directory = test_dir / "summaries"

    dataset_directory = summary_directory / "ds001"
    dataset_directory.mkdir(parents=True)
    (dataset_directory / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t60\t60\n"
    )
    (dataset_directory / "requester_count.tsv").write_text("60\n")

    archive_directory = summary_directory / "archive"
    archive_directory.mkdir(parents=True)
    (archive_directory / "by_region.tsv").write_text(
        "region\tbytes_sent\tnumber_of_requests\tnumber_of_downloads\nmissing\t10\t60\t60\n"
    )
    (archive_directory / "requester_count.tsv").write_text("60\n")

    s3_log_extraction.summarize.generate_all_dataset_totals(cache_directory=test_dir)
    s3_log_extraction.summarize.generate_archive_totals(cache_directory=test_dir)

    totals = json.loads((summary_directory / "totals.json").read_text())
    archive_totals = json.loads((summary_directory / "archive_totals.json").read_text())
    assert totals["ds001"]["total_number_of_views"] == "<50"
    assert archive_totals["total_number_of_views"] == "<50"

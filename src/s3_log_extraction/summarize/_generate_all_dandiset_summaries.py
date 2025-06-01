import pathlib

import dandi.dandiapi


def generate_all_dandiset_summaries(*, summary_directory: str | pathlib.Path) -> None:
    pass


def _summarize_dandiset(
    *,
    dandiset_id: str,
    associated_assets: dict[str, list[dandi.dandiapi.RemoteAsset]],
    summary_directory: pathlib.Path,
    extraction_directory: pathlib.Path,
    index_to_region: dict[int, str],
) -> None:
    pass


def _summarize_dandiset_by_day(
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
):
    pass


def _summarize_dandiset_by_asset(
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
):
    pass


def _summarize_dandiset_by_region(
    *,
    assets: list[dandi.dandiapi.RemoteAsset],
    summary_file_path: pathlib.Path,
    extraction_directory: pathlib.Path,
    index_to_region: dict[int, str],
):
    pass

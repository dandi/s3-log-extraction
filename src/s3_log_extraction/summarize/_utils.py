import pathlib

import pandas

from .globals import REGION_DISCLOSURE_THRESHOLD, REGION_VALUE_COLUMN_NAMES
from ..ip_utils import country_alpha_2_to_alpha_3, is_resolved_region


def _read_integers_from_file(file_path: pathlib.Path, /) -> list[int]:
    """Read one integer per line from an extraction file such as ``bytes_sent.txt`` or ``download.txt``."""
    return [int(value.strip()) for value in file_path.read_text().splitlines()]


def _coerce_activity_columns(summary_table: pandas.DataFrame, /) -> None:
    """
    Make the activity columns of a summary read back as ``int64``, in place.

    A summary written before views were reported has no ``number_of_views`` column, so the missing
    activity columns are backfilled with zero before the whole set is coerced.
    """
    for column_name in ("number_of_requests", "number_of_downloads", "number_of_views"):
        if column_name not in summary_table.columns:  # Summarized before views were reported
            summary_table[column_name] = 0
        summary_table[column_name] = (
            pandas.to_numeric(summary_table[column_name], errors="coerce").fillna(0).astype("int64")
        )


def _build_totals(
    *,
    summary_table: pandas.DataFrame,
    number_of_unique_regions: int,
    number_of_unique_countries: int,
    number_of_requesters: str | int,
) -> dict[str, str | int]:
    """
    Build the published totals of one by-day summary, shared by the archive and per-dataset totals.

    The seven keys here are the schema of both ``totals.json`` and ``archive_totals.json``.
    ``number_of_requesters`` is taken as it was read: a count written by an earlier version may be a
    sentinel string such as ``"<50"``, which carries no number and is passed through unchanged, and the
    per-dataset totals pass the integer zero for a dataset with no requester count file at all.
    """
    if isinstance(number_of_requesters, str) and not number_of_requesters.startswith("<"):
        number_of_requesters = int(number_of_requesters)

    return {
        "total_bytes_sent": int(summary_table["bytes_sent"].sum()),
        "number_of_unique_regions": number_of_unique_regions,
        "number_of_unique_countries": number_of_unique_countries,
        "total_number_of_requests": int(summary_table["number_of_requests"].sum()),
        "total_number_of_downloads": int(summary_table["number_of_downloads"].sum()),
        "number_of_requesters": number_of_requesters,
        "total_number_of_views": int(summary_table["number_of_views"].sum()),
    }


def _read_summary_value(value: str | int | float, /) -> int:
    """
    Read a single value of a by-region summary that was written previously.

    Summaries written by earlier versions censored values below a disclosure threshold with sentinel
    strings such as ``"<50"``. Those carry no number and are read as zero, so that they compare as
    changed against any true value and the summary is republished once enough regions move.
    """
    numeric_value = pandas.to_numeric(value, errors="coerce")
    return 0 if pandas.isna(numeric_value) else int(numeric_value)


def _collect_resolved_region_values(summary_table: pandas.DataFrame, /) -> dict[str, tuple[int, ...]]:
    """
    Reduce a by-region summary to the values of its resolved regions, keyed by region.

    Only resolved regions are collected. Labels such as ``"missing"`` or ``"undetermined"`` aggregate
    requesters whose location is unknown, so they name no place and cannot be counted as one.
    """
    values_by_region: dict[str, list[int]] = {}
    for _, row in summary_table.iterrows():
        region = str(row["region"])
        if not is_resolved_region(region):
            continue

        values = values_by_region.setdefault(region, [0] * len(REGION_VALUE_COLUMN_NAMES))
        for index, column_name in enumerate(REGION_VALUE_COLUMN_NAMES):
            values[index] += _read_summary_value(row[column_name] if column_name in summary_table.columns else 0)
    return {region: tuple(values) for region, values in values_by_region.items()}


def _count_updated_regions(*, summary_table: pandas.DataFrame, previous_summary_table: pandas.DataFrame | None) -> int:
    """
    Count the resolved regions whose values the new summary would change.

    Every resolved region of a summary that has no previous version counts as updated, since writing that
    summary for the first time discloses all of them at once.
    """
    new_values = _collect_resolved_region_values(summary_table)
    if previous_summary_table is None:
        return len(new_values)

    previous_values = _collect_resolved_region_values(previous_summary_table)
    unchanged = (0,) * len(REGION_VALUE_COLUMN_NAMES)
    return sum(
        1
        for region in set(new_values) | set(previous_values)
        if new_values.get(region, unchanged) != previous_values.get(region, unchanged)
    )


def _write_summary_by_region(
    *,
    summary_table: pandas.DataFrame,
    summary_file_path: pathlib.Path,
    region_disclosure_threshold: int = REGION_DISCLOSURE_THRESHOLD,
) -> bool:
    """
    Write a by-region summary only if the update it carries spans more than the threshold of regions.

    Parameters
    ----------
    summary_table : pandas.DataFrame
        The newly computed by-region summary, with its true values.
    summary_file_path : pathlib.Path
        Destination of the summary. Any version already there is read to determine what the new summary
        would change, and is left untouched when the update is withheld.
    region_disclosure_threshold : int
        Number of resolved regions an update must move at once to be published.
        Defaults to ``REGION_DISCLOSURE_THRESHOLD``.

    Returns
    -------
    bool
        Whether the summary was written.
    """
    previous_summary_table = (
        pandas.read_table(filepath_or_buffer=summary_file_path) if summary_file_path.exists() else None
    )
    number_of_updated_regions = _count_updated_regions(
        summary_table=summary_table, previous_summary_table=previous_summary_table
    )
    if number_of_updated_regions <= region_disclosure_threshold:
        return False

    summary_file_path.parent.mkdir(parents=True, exist_ok=True)
    summary_table.to_csv(path_or_buf=summary_file_path, mode="w", sep="\t", header=True, index=False)
    return True


def _count_regions_and_countries(summary_file_path: pathlib.Path, /) -> tuple[int, int]:
    """
    Count the regions and the distinct countries of a by-region summary.

    A by-region summary is published only once its update spans enough resolved regions, so a dataset with
    little activity may not have one yet. Both counts are zero in that case.
    """
    if not summary_file_path.exists():
        return 0, 0

    summary_table = pandas.read_table(filepath_or_buffer=summary_file_path)
    regions = [str(region) for region in summary_table["region"]]

    unique_countries: set[str] = set()
    for region in regions:
        if not is_resolved_region(region):
            continue

        country_code, region_name = region.split("/", 1)
        if "AWS" in country_code:
            # AWS region names start with an alpha-2 country code ("us-east-1"); align it with the alpha-3
            # codes of geographic labels so that the same country is not counted twice
            country_code = country_alpha_2_to_alpha_3(region_name.split("-")[0])
        unique_countries.add(country_code)

    return len(regions), len(unique_countries)

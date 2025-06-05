import ipinfo
import opencage.geocoder


def update_region_code_coordinates() -> None:
    pass


def _get_coordinates_from_region_code(
    *,
    country_and_region_code: str,
    ipinfo_client: ipinfo.Handler,
    opencage_api_key: str,
    service_coordinates: dict[str, dict[str, float]],
    opencage_failures: list[str],
) -> dict[str, float]:
    pass


def _get_service_coordinates_from_ipinfo(
    *,
    country_and_region_code: str,
    ipinfo_client: ipinfo.Handler,
    service_coordinates: dict[str, dict[str, float]],
) -> dict[str, float]:
    pass


def _get_coordinates_from_opencage(
    *, country_and_region_code: str, opencage_client: opencage.geocoder.OpenCageGeocode, opencage_failures: list[str]
) -> dict[str, float]:
    pass

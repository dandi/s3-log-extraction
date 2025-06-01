import typing


def load_index_to_ip() -> dict[int, str]:
    pass


def save_index_to_ip(*, index_to_ip: dict[int, str]) -> None:
    pass


def load_ip_cache(
    *,
    cache_type: typing.Literal["index_to_region", "index_not_in_services"],
) -> dict[int, str]:
    pass

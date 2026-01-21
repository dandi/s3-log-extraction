import pathlib
import random

import numpy
import numpy.random
import tqdm

from ._ip_cache import load_index_to_ip, save_index_to_ip
from ..config import get_cache_directory


def index_ips(*, seed: int = None, cache_directory: str | pathlib.Path | None = None) -> None:
    """
    Indexes IP addresses extracted from the S3 log files.

    This function reads the full IPs from the extracted S3 log files, replacing them with a new file containing
    the randomized indexes of unique IPs.

    The index mapping to full IPs is encrypted and saved to the cache for if access is ever needed for lookup purposes.
    """
    rng = numpy.random.default_rng(seed=seed)
    dtype = "uint64"
    high = numpy.iinfo(dtype).max
    max_redraws = 1_000

    cache_directory = pathlib.Path(cache_directory) if cache_directory is not None else get_cache_directory()
    extraction_directory = cache_directory / "extraction"

    index_to_ip = load_index_to_ip(cache_directory=cache_directory)
    ip_to_index = {ip: index for index, ip in index_to_ip.items()}
    indexed_ips = {ip for ip in index_to_ip.values()}

    full_ip_file_paths = list(extraction_directory.rglob(pattern="*full_ips.txt"))
    random.shuffle(full_ip_file_paths)

    for full_ip_file_path in tqdm.tqdm(
        iterable=full_ip_file_paths, total=len(full_ip_file_paths), desc="Indexing IP files", unit="files", smoothing=0
    ):
        full_ips = [line.strip() for line in full_ip_file_path.read_text().splitlines()]
        unique_full_ips = {ip for ip in full_ips}
        ips_to_index = unique_full_ips - indexed_ips

        for ip in ips_to_index:
            new_index = int(rng.integers(low=0, high=high, dtype=dtype))

            redraw = 0
            while index_to_ip.get(new_index, None) is not None and redraw < max_redraws:
                new_index = int(rng.integers(low=0, high=high, dtype=dtype))
                redraw += 1

            if redraw >= max_redraws:
                message = (
                    f"Failed to find a unique index for an IP after {max_redraws} redraws - "
                    "suggest increasing either index dtype or redraw limit."
                )
                raise ValueError(message)

            index_to_ip[new_index] = ip
            ip_to_index[ip] = new_index
            indexed_ips.update(ip_to_index)

        full_indexed_ips = [f"{ip_to_index[ip]}" for ip in full_ips]

        indexed_ips_file_path = full_ip_file_path.parent / "indexed_ips.txt"
        indexed_ips_file_path.write_text("\n".join(full_indexed_ips))

    save_index_to_ip(index_to_ip=index_to_ip, cache_directory=cache_directory)

    # TODO: add delayed optional cleanup
    # for full_ip_file_path in full_ip_file_paths:
    #     full_ip_file_path.unlink()

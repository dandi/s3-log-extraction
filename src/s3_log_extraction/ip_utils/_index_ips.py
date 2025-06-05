import hashlib

import numpy
import numpy.random
import tqdm

from ._ip_cache import load_index_to_ip, save_index_to_ip
from ..config import get_cache_directory


def index_ips(*, seed: int = 0) -> None:
    """
    Indexes IP addresses extracted from the S3 log files.

    This function reads the full IPs from the extracted S3 log files, replacing them with a new file containing
    the randomized indices of unique IPs.

    The index mapping to full IPs is encrypted and saved to the cache for if access is ever needed for lookup purposes.
    """
    cache_directory = get_cache_directory()
    extraction_directory = cache_directory / "extraction"

    index_to_ip = load_index_to_ip()
    ip_to_index = {value: key for key, value in index_to_ip.items()}

    full_ip_file_paths = list(extraction_directory.rglob(pattern="*full_ips.txt"))
    for full_ip_file_path in tqdm.tqdm(
        iterable=full_ip_file_paths, total=len(full_ip_file_paths), desc="Indexing IP files", unit="file", smoothing=0
    ):
        full_ips = {ip for ip in numpy.loadtxt(fname=full_ip_file_path, dtype="U15", ndmin=1)}
        new_indices_and_uniqueness = {
            (index := int(hashlib.sha1(string=bytes(ip, "utf-8")).hexdigest()[:7]), 16): index_to_ip.get(index, False)
            for ip in full_ips
        }

        if not all(new_indices_and_uniqueness.values()):
            message = "Index collision detected - recommend raising the dtype and hash limit."
            raise ValueError(message)

        for ip, index in zip(full_ips, new_indices_and_uniqueness.keys()):
            index_to_ip[int(index)] = str(ip)
            ip_to_index[ip] = index

        full_indexed_ips = [ip_to_index[ip] for ip in full_ips]

        indexed_ips_file_path = full_ip_file_path.parent / "indexed_ips.txt"
        with indexed_ips_file_path.open(mode="w") as file_stream:
            numpy.savetxt(fname=file_stream, X=full_indexed_ips, fmt="%d")

    # TODO: add validation for unexpected ip file combinations
    save_index_to_ip(index_to_ip=index_to_ip)

    # TODO: add delayed optional cleanup
    # for full_ip_file_path in full_ip_file_paths:
    #     full_ip_file_path.unlink()

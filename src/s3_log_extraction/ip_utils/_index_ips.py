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
    rng = numpy.random.default_rng(seed=seed)

    cache_directory = get_cache_directory()
    extraction_directory = cache_directory / "extraction"

    index_to_ip = load_index_to_ip()
    ip_to_index = {value: key for key, value in index_to_ip.items()}

    # Using the upper bound of uint16 as current limit; not expecting radically larger number of users
    # TODO: add validation to notify if we get close to this
    all_possible_indices = set(range(1, 65_536))
    used_indices = set(index_to_ip.keys())

    full_ip_file_paths = list(extraction_directory.rglob(pattern="*full_ips.txt"))
    for full_ip_file_path in tqdm.tqdm(
        iterable=full_ip_file_paths, total=len(full_ip_file_paths), desc="Indexing IP files", unit="file", smoothing=0
    ):
        full_ips = numpy.loadtxt(fname=full_ip_file_path, dtype="U15", ndmin=1)
        new_ips = set(full_ips) - set(ip_to_index.keys())

        available_indices = list(all_possible_indices - used_indices)
        new_indices = rng.choice(a=available_indices, size=len(new_ips), replace=False, shuffle=False)
        used_indices.update(new_indices)

        for new_ip, new_index in zip(new_ips, new_indices):
            index_to_ip[new_index] = new_ip
            ip_to_index[new_ip] = new_index

        full_indexed_ips = numpy.array(object=[ip_to_index[ip] for ip in full_ips], dtype="uint16")

        indexed_ips_file_path = full_ip_file_path.parent / "indexed_ips.bin"
        numpy.save(file=indexed_ips_file_path, arr=full_indexed_ips, allow_pickle=False)
        full_ip_file_path.unlink()

    # TODO: add validation for unexpected ip file combinations
    save_index_to_ip(index_to_ip=index_to_ip)

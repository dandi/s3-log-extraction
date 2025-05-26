import numpy
import numpy.random
import yaml

from ..config import get_cache_directory
from ..encryption_utils import decrypt_bytes, encrypt_bytes


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
    ips_cache_directory = cache_directory / "ips"
    ips_index_cache_file_path = ips_cache_directory / "indexed_ips.yaml"

    if not ips_index_cache_file_path.exists():
        ips_index_cache_file_path.touch()

    with ips_index_cache_file_path.open(mode="rb") as file_stream:
        encrypted_content = file_stream.read()
    decrypted_content = decrypt_bytes(encrypted_data=encrypted_content)
    index_to_ip = yaml.safe_load(stream=decrypted_content) or {}
    ip_to_index = {value: key for key, value in index_to_ip.items()}

    # Using the upper bound of uint16 as current limit; not expecting radically larger number of users
    # TODO: add validation to notify if we get close to this
    all_possible_indices = set(range(1, 65_536))
    used_indices = set(index_to_ip.keys())

    for full_ip_file_path in extraction_directory.rglob(pattern="*full_ips.txt"):
        full_ips = numpy.loadtxt(fname=full_ip_file_path, dtype="U15")
        unique_ips = numpy.unique(full_ips)

        available_indices = all_possible_indices - used_indices
        new_indices = rng.choice(a=available_indices, size=len(unique_ips), replace=False, shuffle=False)
        used_indices.update(new_indices)

        for ip, new_index in zip(unique_ips, new_indices):
            index_to_ip[new_index] = ip
            ip_to_index[ip] = new_index

        full_indexed_ips = numpy.array(object=[ip_to_index[ip] for ip in full_ips], dtype="uint16")

        indexed_ips_file_path = full_ip_file_path.parent / "indexed_ips.bin"
        numpy.save(file=indexed_ips_file_path, arr=full_indexed_ips, allow_pickle=False)
        full_ip_file_path.unlink()

    encrypted_content = encrypt_bytes(data=yaml.dumps(index_to_ip).encode(encoding="utf-8"))
    with ips_index_cache_file_path.open(mode="wb") as file_stream:
        file_stream.write(encrypted_content)

def _index_ips(seed: int = 0) -> None:
    """
    Indexes IP addresses extracted from the S3 log files.

    This function reads the full IPs from the extracted S3 log files, replacing them with a new file containing
    the randomized indices of unique IPs.

    The index mapping to full IPs is encrypted and saved to the cache for if access is ever needed for lookup purposes.
    """
    # Set numpy seed
    pass
    # random_number_generator = numpy.random.default_rng(seed=seed)
    #
    # cache_directory = get_cache_directory()
    # extraction_directory = cache_directory / "extraction"
    #
    # for full_ip_file_path in extraction_directory.glob("*full_ips.txt"):
    #     full_ips = numpy.loadtxt(fname=full_ip_file_path, dtype="U15")
    #
    #     unique_ips = numpy.unique(full_ips)

    # random_indices = random_number_generator.choice(a=1, size=len(unique_ips), replace=False, shuffle=False)
    #
    # indexed_ip_file_path = full_ip_file_path.parent / "indexed_ips.txt"

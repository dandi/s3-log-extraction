import os
import warnings


def _handle_max_workers(*, workers: int) -> int:
    """
    Handle the number of workers for parallel processing.

    If workers is 0, it raises a warning and sets it to -2 (default).
    If workers is negative, it calculates the maximum number of workers based on CPU count.
    If workers is positive, it ensures it does not exceed the CPU count.
    """
    if workers == 0:
        message = "The number of workers cannot be 0 - please set it to an integer. Falling back to default of -2."
        warnings.warn(message=message, stacklevel=2)
        workers = -2

    cpu_count = os.cpu_count()
    if workers < 0:
        max_workers = workers % cpu_count + 1
    elif workers > cpu_count:
        max_workers = cpu_count
    else:
        max_workers = workers

    return max_workers

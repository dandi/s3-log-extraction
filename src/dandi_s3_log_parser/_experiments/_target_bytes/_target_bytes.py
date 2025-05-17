import numpy


def seek_and_read(io, pos: int, amount: int) -> bytes:
    """
    Seek to a position in the file and read a specified amount of bytes.
    """
    io.seek(pos)
    return io.read(amount)


def target_bytes(filename: str, ranges: list[int]) -> None:
    with open(filename, "rb") as io:
        all_data = [seek_and_read(io=io, pos=range + 107, amount=15) for range in ranges]

    print(all_data[:5])


if __name__ == "__main__":
    # awk '{ print length }' /mnt/backup/dandi/dandiarchive-logs/2021/10/04.log
    # > /mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt
    range_file = "/mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt"
    with open(range_file, "r") as io:
        lengths = [int(line.strip()) + 1 for line in io.readlines()]
    lengths.insert(0, 0)
    ranges = numpy.cumsum(lengths)

    filename = "/mnt/backup/dandi/dandiarchive-logs/2021/10/04.log"
    target_bytes(filename=filename, ranges=ranges)

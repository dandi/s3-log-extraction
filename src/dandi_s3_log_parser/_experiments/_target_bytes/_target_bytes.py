import typing

SPACE_BYTE = b" "[0]


def seek_and_read(io: typing.BinaryIO, pos: int, amount: int) -> bytes:
    """
    Seek to a position in the file and read a specified amount of bytes.
    """
    io.seek(pos)
    return io.read(amount)


def strip_space(byte_input: bytes) -> bytes:
    # Shortest IP is 0.0.0.0 (7)
    # Longest is 256.256.256.256 (16)
    for counter in range(7, 16):
        if byte_input[counter] == SPACE_BYTE:
            return byte_input[:counter]


def get_ip(io: typing.BinaryIO, pos: int) -> str:
    byte_input = seek_and_read(io=io, pos=pos + 107, amount=16)
    without_space = strip_space(byte_input=byte_input)
    return without_space.decode(encoding="utf-8")


def target_bytes(filename: str, offsets: list[int]) -> None:
    with open(filename, "rb") as io:
        all_ips = [get_ip(io=io, pos=pos) for pos in offsets]

    print(all_ips[:5])


if __name__ == "__main__":
    # awk '{ print length - 1 }' /mnt/backup/dandi/dandiarchive-logs/2021/10/04.log \
    #   | awk '{s+=$1; print s}' > /mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt
    range_file = "/mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt"
    with open(range_file, "r") as io:
        offsets = [int(line.strip()) for line in io.readlines()]

    filename = "/mnt/backup/dandi/dandiarchive-logs/2021/10/04.log"
    target_bytes(filename=filename, offsets=offsets)

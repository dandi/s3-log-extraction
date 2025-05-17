import typing

DASH = "-"
SPACE = " "


def seek_and_read(io: typing.TextIO, pos: int, amount: int) -> str:
    """
    Seek to a position in the file and read a specified amount of bytes.
    """
    io.seek(pos)
    return io.read(amount)


def get_ip(io: typing.TextIO, pos: int) -> str:
    first_character = seek_and_read(io=io, pos=pos + 107, amount=1)
    if first_character == DASH:
        return first_character

    str_input = seek_and_read(io=io, pos=pos + 107, amount=16)
    without_space = str_input.split(SPACE)[0]
    return without_space


def target_str(filename: str, offsets: list[int]) -> None:
    with open(filename, "r") as io:
        all_ips = [get_ip(io=io, pos=pos) for pos in offsets]

    print(all_ips[:5])


if __name__ == "__main__":
    range_file = "/mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt"
    with open(range_file, "r") as io:
        offsets = [int(line.strip()) for line in io.readlines()]

    filename = "/mnt/backup/dandi/dandiarchive-logs/2021/10/04.log"
    target_str(filename=filename, offsets=offsets)

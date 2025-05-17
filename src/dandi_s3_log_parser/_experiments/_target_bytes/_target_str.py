import typing


def seek_and_read(text_io: typing.TextIO, pos: int, amount: int) -> str:
    """
    Seek to a position in the file and read a specified amount of bytes.
    """
    io.seek(pos)
    return io.read(amount)


def get_ip(text_io: typing.TextIO, pos: int) -> str:
    str_input = seek_and_read(text_io=text_io, pos=pos + 106, amount=16)
    without_space = str_input.split(" ")[0]
    return without_space


def target_str(filename: str, offsets: list[int]) -> None:
    with open(filename, "r") as text_io:
        all_ips = [get_ip(text_io=text_io, pos=pos) for pos in offsets]

    print(all_ips[:3])


if __name__ == "__main__":
    range_file = "/mnt/backup/dandi/dandiarchive-logs-cody/test/test_ranges.txt"
    with open(range_file, "r") as io:
        offsets = [int(line.strip()) for line in io.readlines()]

    filename = "/mnt/backup/dandi/dandiarchive-logs/2021/10/04.log"
    target_str(filename=filename, offsets=offsets)

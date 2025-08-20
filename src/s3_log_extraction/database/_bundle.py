import itertools
import pathlib

import polars
import tqdm
import yaml

from ..config import get_cache_directory


def bundle_database():
    """
    Bundles the current results of the extraction cache into a hive-partitioned Parquet database.

    This makes it easier to share publicly.
    """
    cache_directory = get_cache_directory()
    extraction_directory = cache_directory / "extraction"
    sharing_directory = cache_directory / "sharing"
    sharing_directory.mkdir(exist_ok=True)

    all_asset_timestamps_file_paths = itertools.chain(
        (extraction_directory / asset_type).rglob(pattern="timestamps.txt") for asset_type in ["blobs", "zarr"]
    )

    # We here refer to all IDs as 'blob IDs', even if they are Zarr assets
    blob_index_to_id = {
        blob_index: timestamps_file_path.parent.name
        for blob_index, timestamps_file_path in enumerate(all_asset_timestamps_file_paths)
    }
    blob_id_to_index = {value: key for key, value in blob_index_to_id.items()}

    for asset_type in ["blobs", "zarr"]:
        asset_type_key = asset_type[0]
        asset_partition_directory = sharing_directory / f"asset_type={asset_type}"
        asset_partition_directory.mkdir(exist_ok=True)

        for blob_head in itertools.chain(range(10), list("abcdef")):
            blob_head_partition_directory = asset_partition_directory / f"blob_head={blob_head}"
            blob_head_partition_directory.mkdir(exist_ok=True)

            timestamps_file_paths_per_partition = (extraction_directory / asset_type).rglob(
                pattern=f"{blob_head}*/**/timestamps.txt"
            )

            all_asset_types = []  # only the first character ('b' for blobs or 'z' for Zarr) is used to save RAM
            all_blob_heads = []  # 'head' = 'first character of ID'
            all_timestamps = []
            all_blob_indexes = []
            all_bytes_sent = []
            all_indexed_ips = []
            for timestamps_file_path in tqdm.tqdm(
                iterable=timestamps_file_paths_per_partition,
                total=len(timestamps_file_paths_per_partition),
                desc=f"Reading asset data for type {asset_type}/{blob_head}*",
                mininterval=5.0,
                smoothing=0,
                unit="assets",
            ):
                asset_directory = timestamps_file_path.parent
                blob_id = asset_directory.name
                blob_index = blob_id_to_index[blob_id]

                timestamps = _load_content(file_path=timestamps_file_path)
                bytes_sent = _load_content(file_path=asset_directory / "bytes_sent.txt")
                indexed_ips = _load_content(file_path=asset_directory / "indexed_ips.txt")

                all_asset_types.extend([asset_type_key] * len(timestamps))
                all_blob_heads.extend([blob_id[0]] * len(timestamps))

                all_timestamps.extend(timestamps)
                all_blob_indexes.extend([blob_index] * len(timestamps))
                all_bytes_sent.extend(bytes_sent)
                all_indexed_ips.extend(indexed_ips)

            data_frame = polars.DataFrame(
                data={
                    "asset_type": all_asset_types,
                    "blob_head": all_blob_heads,
                    "timestamp": all_timestamps,
                    "blob_index": all_blob_indexes,
                    "bytes_sent": all_bytes_sent,
                    "indexed_ip": all_indexed_ips,
                }
            )

            database_file_path = blob_head_partition_directory / "0.parquet"
            data_frame.write_parquet(file=database_file_path)

    blob_index_to_id_file_path = sharing_directory / "blob_index_to_id.yaml"
    with blob_index_to_id_file_path.open(mode="w") as file_stream:
        yaml.dump(data=blob_index_to_id, stream=file_stream)


def _load_content(file_path: pathlib.Path) -> list[int]:
    with file_path.open(mode="r") as file_stream:
        return [int(line.strip()) for line in file_stream.readlines()]

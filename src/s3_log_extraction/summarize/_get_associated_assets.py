import collections
import datetime

import dandi.dandiapi
import yaml

from ..config import get_cache_directory


def _get_associated_assets(use_cache: bool = True) -> dict[str, list[dandi.dandiapi.RemoteAsset]]:
    cache_directory = get_cache_directory()
    dandi_cache_directory = cache_directory / "dandi"
    dandi_cache_directory.mkdir(exist_ok=True)

    client = dandi.dandiapi.DandiAPIClient()

    date = datetime.datetime.now().date().strftime("%Y-%m-%d")
    daily_dandi_cache_file_path = dandi_cache_directory / f"{date}.yaml"
    if use_cache is True and daily_dandi_cache_file_path.exists():
        with daily_dandi_cache_file_path.open(mode="r") as file_stream:
            asset_id_to_dandiset_ids = yaml.safe_load(stream=file_stream)
        asset_id_to_asset = {
            asset_id: client.get_asset(asset_id=asset_id) for asset_id in asset_id_to_dandiset_ids.keys()
        }
    else:
        base_dandisets = client.get_dandisets()
        all_dandisets = [
            client.get_dandiset(dandiset_id=dandiset.identifier, version_id=version.identifier)
            for dandiset in base_dandisets
            for version in dandiset.get_versions()
        ]

        asset_id_to_dandiset_ids = collections.defaultdict(set)
        asset_id_to_asset = {}
        for dandiset in all_dandisets:
            for asset in dandiset.get_assets():
                asset_id_to_dandiset_ids[asset.identifier].update([dandiset.identifier])
                asset_id_to_asset[asset.identifier] = asset

        with daily_dandi_cache_file_path.open(mode="w") as file_stream:
            yaml.dump(data=dict(asset_id_to_dandiset_ids), stream=file_stream)

    # TODO: add validation or other automatic cleaner for old caches here

    uniquely_associated_assets_by_dandiset_id = collections.defaultdict(list)
    for asset_id, dandiset_ids in asset_id_to_dandiset_ids.items():
        asset = asset_id_to_asset[asset_id]
        if len(dandiset_ids) > 1:
            uniquely_associated_assets_by_dandiset_id["undetermined"].append(asset)
        else:
            dandiset_id = list(dandiset_ids)[0]
            uniquely_associated_assets_by_dandiset_id[dandiset_id].append(asset)

    return uniquely_associated_assets_by_dandiset_id

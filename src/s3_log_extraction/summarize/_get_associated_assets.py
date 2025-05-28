import collections

import dandi.dandiapi


def _get_associated_assets() -> dict[str, list[dandi.dandiapi.RemoteAsset]]:
    # TODO: cache published dandisets to avoid repeating web requests

    client = dandi.dandiapi.DandiAPIClient()
    base_dandisets = client.get_dandisets()
    all_dandisets = [
        client.get_dandiset(dandiset_id=dandiset.identifier, version_id=version.identifier)
        for dandiset in base_dandisets
        for version in dandiset.get_versions()
    ]

    asset_id_to_dandiset_ids = collections.defaultdict(set)
    for dandiset in all_dandisets:
        for asset in dandiset.get_assets():
            asset_id_to_dandiset_ids[asset.identifier].update([dandiset.identifier])

    uniquely_associated_assets_by_dandiset_id = collections.defaultdict(list)
    for asset_id, dandiset_ids in asset_id_to_dandiset_ids.items():
        if len(dandiset_ids) > 1:
            continue

        dandiset_id = list(dandiset_ids)[0]
        uniquely_associated_assets_by_dandiset_id[dandiset_id].append(asset)

    return uniquely_associated_assets_by_dandiset_id

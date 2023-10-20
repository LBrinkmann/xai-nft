import os
import pandas as pd
from ..utils.data import load_json, get_all_files


def parse_collections(top_folder):
    # merge details of top collections

    details_folder = os.path.join(top_folder, 'details')
    # Filter files to include only JSON files
    details_files = [f for f in get_all_files(details_folder) if f.endswith('.json')]

    collection_details = [load_json(cf)['collection'] for cf in details_files]

    df_meta = pd.json_normalize([{k: v for k, v in c.items() if k not in [
                                'editors', 'payment_tokens', 'primary_asset_contracts', 'traits', 'stats']} for c in collection_details])
    df_stats = pd.DataFrame(
        [{'name': c['name'], 'slug': c['slug'], **c['stats']} for c in collection_details])

    df_meta.to_csv(os.path.join(
        top_folder, 'meta.csv'), index=False)
    df_stats.to_csv(os.path.join(
        top_folder, 'stats.csv'), index=False)

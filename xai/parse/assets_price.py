import os
import pandas as pd
from ..utils.data import load_json, get_all_files


def get_sales_info(sale):
    if sale is None:
        return {}
    else:
        pt = sale['payment_token'] if sale['payment_token'] else {}
        return {
            'currency_symbol': pt.get('symbol'),
            'asset_bundle': sale['asset_bundle'],
            'event_timestamp': sale['transaction']['timestamp'],
            'event_type': sale['event_type'],
            'total_price': sale['total_price'],
            'currency_name': pt.get('name'),
            'eth_price': pt.get('eth_price'),
            'usd_price': pt.get('usd_price'),
            'quantity': sale['quantity'],
            'decimals':  pt.get('decimals'),
        }


def get_asset_info(a):
    asset_info = {
        "asset_id": a['id'],
        "token_id": a['token_id'],
        "asset_contract_address": a['asset_contract']['address'],
        "collection": a['collection']['slug'],
        'image_url': a['image_url'],
        'image_preview_url': a['image_preview_url'],
        'image_thumbnail_url': a['image_thumbnail_url'],
    }
    return asset_info


def process_assets(top_folder):
    # merge assets
    sub_folder = 'assets'

    folder = os.path.join(top_folder, sub_folder)
    files = list(get_all_files(folder))

    traits = []
    sales = []
    assets = []
    for cf in files:
        raw_assets = load_json(cf)
        for a in raw_assets:
            try:
                traits.extend({"asset_id": a['id'], **t} for t in a['traits'])
                sales.append(
                    {
                        **get_asset_info(a),
                        **get_sales_info(a['last_sale'])})
                assets.append(get_asset_info(a))
            except Exception as e:
                print(a)
                raise e

    df_traits = pd.DataFrame(traits)
    df_sales = pd.DataFrame(sales)
    df_assets = pd.DataFrame(assets)

    df_traits = post_traits(df_traits)
    df_sales = post_sales(df_sales)

    df_traits.to_parquet(
        os.path.join(top_folder, 'traits.parquet'), engine='pyarrow')
    df_sales.to_parquet(os.path.join(top_folder,
                        'last_sales.parquet'), engine='pyarrow')
    df_assets.to_parquet(os.path.join(top_folder,
                                      'assets.parquet'), engine='pyarrow')


def post_traits(df):
    traits_columns = ['asset_id', 'trait_type', 'value', 'trait_count']
    df['value'] = df['value'].astype('str')
    df['trait_type'] = df['trait_type'].astype('str')
    return df[traits_columns]


def post_sales(df):
    df['total_price'] = df['total_price'].astype(float)
    df['quantity'] = df['quantity'].astype(float)
    df['eth_price'] = df['eth_price'].astype(float)

    df['price_eth'] = df['total_price'] * \
        df['quantity'] * df['eth_price'] / 10**df['decimals']
    df['timestamp'] = pd.to_datetime(df['event_timestamp'])
    return df


def process_sales(top_folder):
    # merge events
    sub_folder = 'events'

    folder = os.path.join(top_folder, sub_folder)
    files = list(get_all_files(folder))

    rec = []
    for i, f in enumerate(files):
        if i % (len(files) // 5) == 0:
            print(f'{i}/{len(files)}')
        raw = load_json(f)
        for el in raw:
            if el['asset'] is not None:
                try:
                    rec.append({
                        "asset_id": el['asset']['id'],
                        "token_id": el['asset']['token_id'],
                        "asset_contract_address": el['asset']['asset_contract']['address'],
                        "collection": el['asset']['collection']['slug'],
                        **get_sales_info(el)})
                except:
                    print(el)
                    raise

    df = pd.DataFrame(rec)
    df = post_sales(df)
    df.to_parquet(os.path.join(top_folder, 'sale_events.parquet'), index=False)


def merge_sales(top_folder):
    df_ls = pd.read_parquet(os.path.join(
        top_folder, 'last_sales.parquet'), engine='pyarrow')
    df_e = pd.read_parquet(os.path.join(
        top_folder, 'sale_events.parquet'), engine='pyarrow')
    # df_a = pd.read_parquet(os.path.join(
    #     top_folder, 'assets.parquet'), engine='pyarrow')
    df_ls['origin'] = 'last_sale'
    df_e['origin'] = 'events'

    df = pd.concat([df_ls, df_e])
    df = df.dropna(subset=['price_eth'])
    df = df.drop_duplicates(subset=['event_timestamp', 'asset_id'])
    # df = df.merge(df_a[['asset_id', 'collection']], how='left')

    df.to_parquet(os.path.join(top_folder, 'sales.parquet'), engine='pyarrow')

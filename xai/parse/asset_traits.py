import os
import pandas as pd


def enrich_traits(top_folder):

    df_t = pd.read_parquet(os.path.join(top_folder, 'traits.parquet'), engine='pyarrow')
    df_a = pd.read_parquet(os.path.join(top_folder, 'assets.parquet'), engine='pyarrow')
    df_t = df_t.drop_duplicates(subset=['asset_id', 'trait_type', 'value'])

    df_a = df_a.drop_duplicates(subset=['asset_id'])
    df_a['asset_count'] = df_a.groupby(['collection'])['asset_id'].transform('count')

    df_t = df_t.merge(df_a[['asset_id', 'collection', 'asset_count']])

    trait_count = df_t.groupby(['asset_id', 'collection', 'asset_count'])['trait_type'].count()
    trait_count.name = 'value'
    trait_count = trait_count.reset_index()
    trait_count['trait_type'] = 'TraitCount'
    df_t = pd.concat([df_t, trait_count])

    df_t['trait_type'] = df_t['trait_type'].astype(str)
    df_t['value'] = df_t['value'].astype(str)

    df_t['trait_id'] = df_t['trait_type'] + '_' + df_t['value']

    df_t['trait_count'] = df_t.groupby(['collection', 'trait_id'])['trait_type'].transform('count')
    df_t['rarity_score'] = df_t['asset_count'] / df_t['trait_count']
    df_t = df_t[df_t['asset_count'] != df_t['trait_count']]

    columns = ['asset_id', 'trait_id', 'trait_type', 'trait_value', 'trait_count', 'rarity_score']

    df_t['trait_value'] = df_t['value'].astype(str)
    df_t[columns].to_parquet(os.path.join(top_folder, 'asset_traits.parquet'), engine='pyarrow')

    columns = ['collection', 'trait_id', 'trait_type',
               'trait_value', 'trait_count', 'rarity_score']
    df_tr = df_t.drop_duplicates(subset=['collection', 'trait_type', 'trait_value'])
    df_tr[columns].to_parquet(os.path.join(
        top_folder, 'traits_enriched.parquet'), engine='pyarrow')

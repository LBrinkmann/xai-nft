import time
import requests
import os
import pandas as pd
import datetime
from time import sleep
from datetime import datetime, timedelta
from ..utils.data import save_json, load_json, check_file, get_all_files

data_folder = '../data'
coll_folder = 'top_collections_3'


def _authorized_opensea_get(url):
    
    opensea_key = '0f100bc5cbc942b2a012ef995fa26ee9'
    
    headers = {
        "Accept": "application/json",
        "X-API-KEY": opensea_key
    }

    response = requests.request("GET", url, headers=headers)
    return response





def _get_collections(limit, offset):
    url = f"https://api.opensea.io/api/v1/collections?offset={offset}&limit={limit}"
    response = _authorized_opensea_get(url)
    return response


def _get_assets(collection, limit, offset):
    url = f"https://api.opensea.io/api/v1/assets?order_direction=desc&offset={offset}&limit={limit}&collection={collection}"
    response = _authorized_opensea_get(url)
    return response


def _get_events(collection, limit, *, cursor, before, after):
    url = (f"https://api.opensea.io/api/v1/events?limit={limit}" +
           f"&event_type=successful&only_opensea=false&collection_slug={collection}")
    if before is not None:
        before = int(before.timestamp())
        url += f"&occurred_before={before}"
    if after is not None:
        after = int(after.timestamp())
        url += f"&occurred_after={after}"
    if cursor is not None:
        url += f"&cursor={cursor}"
    response = _authorized_opensea_get(url)
    return response


def _get_collection(collection):
    url = f"https://api.opensea.io/api/v1/collection/{collection}"
    response = _authorized_opensea_get(url)
    return response



def paginate_cursor(func):
    def f(*, limit, **kwargs):
        cursor = None
        i = 0
        while True:
            resp = func(limit=limit, cursor=cursor, **kwargs)
            cursor = resp['next']
            yield resp
            if cursor == None:
                break
            i += 1
            print(i)
    return f


def paginate(func):
    def f(*, limit, page, **kwargs):
        return func(limit=limit, offset=page*limit, **kwargs)
    return f


def detect_throttle(func):
    def f(*args, **kwargs):
        while True:
            try:
                response = func(*args, **kwargs)
                json = response.json()
                status_code = response.status_code
            except Exception:
                if response.status_code == 404:
                    raise ValueError('Cannot find resource.')
                print(response.status_code, response.text)
                sleep(60)
            else:
                if status_code == 200:
                    return json
                elif status_code == 404:
                    raise ValueError('Cannot find resource.')
                else:
                    print(response.status_code, response.text)
                    sleep(60)

    return f


get_collection = detect_throttle(_get_collection)
get_collections = paginate(
    detect_throttle(_get_collections))
get_assets = paginate(
    detect_throttle(_get_assets))
get_events = paginate_cursor(
    detect_throttle(_get_events))




def get_collection_assets(collection):
    assets = []
    for i in range(51):
        resp = get_assets(collection=collection, limit=100, page=i)
        if len(resp['assets']) == 0:
            break
        assets.extend(resp['assets'])
  
    return assets

def get_all_events(collection, *, before, after):
    events = []
    limit = 200

    for res in get_events(
        collection=collection, limit=limit, before=before, after=after):
        events.extend(res['asset_events'])
        if len(res['asset_events']) != 200:
            break
    return events

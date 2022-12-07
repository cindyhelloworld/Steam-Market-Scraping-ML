# Packages
import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Time
def target_date_to_datetime(target_date, date_format):
    target_datetime = datetime.strptime(target_date, date_format)
    return target_datetime

def datetime_to_timestamp(date_as_datetime):
    date_as_timestamp = int(datetime.timestamp(date_as_datetime))
    return date_as_timestamp

def get_yesterday_datetime(date_as_datetime, days=1):
    yesterday_datetime = date_as_datetime - timedelta(days)
    return yesterday_datetime


# Download
def get_steam_api_url(app_id):
    return f"https://store.steampowered.com/appreviews/{app_id}"

def get_request_params(target_timestamp):
    # References:
    # - https://partner.steamgames.com/doc/store/getreviews
    if target_timestamp is None:
        raise Exception("Empty target_timestamp!")

    params = {
        "json": "1",
        "num_per_page": "0",  # text content of reviews is not needed
        # caveat: default seems to be "english", so reviews would be missing if unchanged!
        "language": "all",
        # caveat: default is "steam", so reviews would be missing if unchanged!
        "purchase_type": "all",
        # to un-filter review-bombs, e.g. https://store.steampowered.com/app/481510/
        "filter_offtopic_activity": "0",
        "start_date": "1",  # this is the minimal value which allows to filter by date
        "end_date": str(target_timestamp),
        "date_range_type": "include",  # this parameter is needed to filter by date
    }

    return params

def download_review_stats(app_id, target_timestamp):
    url = get_steam_api_url(app_id)
    params = get_request_params(target_timestamp)
    response = requests.get(url, params=params)

    if response.ok:
        result = response.json()
    else:
        result = None

    return result


# Save
def get_info(res):
    fields = ['total_reviews', 'total_positive',
              'total_negative', 'review_score']
    query_summary = res["query_summary"]
    info = dict()
    for key in fields:
        info[key] = query_summary[key]

    return info

def download_to_df(app_id, target_date, rate_limits, threshold, date_format="%Y-%m-%d"):
    days = 1
    target_datetime = target_date_to_datetime(target_date, date_format)
    target_timestamp = datetime_to_timestamp(target_datetime)
    res = download_review_stats(app_id, target_timestamp)
    if not res:
        print(f"Invalid appid {app_id} !!!")
        return

    info = get_info(res)
    data = dict()
    # Skip Games with reviews lower than threshold
    if info.get('total_reviews') <= threshold:
        print(f"Review count {info.get('total_reviews')} lower than {threshold}, skip {app_id} !!!")
        return
    info.get('total_reviews')
    query_count = 1
    last_review_count = info.get('total_reviews')
    while (last_review_count):
        print(query_count)
        if query_count % rate_limits["max_num_queries"] == 0:
            cooldown_duration = rate_limits["cooldown"]
            print(f"#queries {query_count} reached. Cooldown: {cooldown_duration} s")
            time.sleep(cooldown_duration)
        data[target_datetime.strftime(date_format)] = info
        target_datetime = get_yesterday_datetime(target_datetime, days)
        target_timestamp = datetime_to_timestamp(target_datetime)
        res = download_review_stats(app_id, target_timestamp)
        if not res:
            print(f"Download Error, appid {app_id} !!!")
            return
        info = get_info(res)
        # When review count not change, skip 2 days a round
        new_review_count = info.get('total_reviews')
        if new_review_count == last_review_count:
            days = 2
        else:
            days = 1
        last_review_count = new_review_count
        query_count += 1
        time.sleep(0.1)

    df = pd.DataFrame.from_dict(data, orient='index')
    df.index.name = "Date"

    return df

def save_df(df, target_dir, appid):
    path = os.path.join(target_dir, str(appid)+".csv")
    df.to_csv(path, encoding="utf-8")


# Main Download
def download_review_to_csv(appid_list, start_date, target_dir, rate_limits, threshold):
    unfinished_task = set(appid_list)
    for appid in appid_list:
        print(f"Downloading {appid}...")
        review_df = download_to_df(appid, start_date, rate_limits, threshold)
        if not review_df:
            continue
        save_df(review_df, target_dir, appid)
        unfinished_task = unfinished_task.difference(appid)
    np.savetxt("unfinished.txt", list(unfinished_task))

def get_appid_list(task_path):
    return np.loadtxt(task_path, dtype='uint32')

if __name__ == "__main__":
    rate_limits = {
        "max_num_queries": 150,
        "cooldown": (2 * 60) + 8,  # 2 minutes plus a cushion,
    }
    threshold = 50
    start_date = "2022-10-22"
    target_dir = "./data"
    os.makedirs(target_dir, exist_ok=True)

    task_path = "./task/test.txt"
    appid_list = get_appid_list(task_path)
    # print(appid_list)

    download_review_to_csv(appid_list, start_date, target_dir, rate_limits, threshold)

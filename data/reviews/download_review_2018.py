import json
import time
import csv
import requests
from datetime import datetime

def get_target_date():
    return "2018-07-01"


def get_date_format():
    return "%Y-%m-%d"

def get_target_date_as_datetime(target_date=None, date_format=None):
    if target_date is None:
        target_date = get_target_date()

    if date_format is None:
        date_format = get_date_format()

    target_datetime = datetime.strptime(target_date, date_format)

    return target_datetime

def convert_from_datetime_to_timestamp(date_as_datetime, verbose=True):
    date_as_timestamp = int(datetime.timestamp(date_as_datetime))

    if verbose:
        print(f"Unix timestamp: {date_as_timestamp}")

    return date_as_timestamp

def get_target_date_as_timestamp(target_date=None, date_format=None, verbose=True):
    target_datetime = get_target_date_as_datetime(target_date, date_format)
    target_timestamp = convert_from_datetime_to_timestamp(
        target_datetime, verbose=verbose
    )

    return target_timestamp

def get_steam_api_url(app_id):
    return f"https://store.steampowered.com/appreviews/{app_id}"


def get_request_params(target_timestamp=None, verbose=True):
    # References:
    # - https://partner.steamgames.com/doc/store/getreviews
    # - browser dev tools on store pages, e.g. https://store.steampowered.com/app/570/#app_reviews_hash

    if target_timestamp is None:
        target_timestamp = get_target_date_as_timestamp(verbose=verbose)

    params = {
        "json": "1",
        "num_per_page": "0",  # text content of reviews is not needed
        "language": "all",  # caveat: default seems to be "english", so reviews would be missing if unchanged!
        "purchase_type": "all",  # caveat: default is "steam", so reviews would be missing if unchanged!
        "filter_offtopic_activity": "0",  # to un-filter review-bombs, e.g. https://store.steampowered.com/app/481510/
        "start_date": "1",  # this is the minimal value which allows to filter by date
        "end_date": str(target_timestamp),
        "date_range_type": "include",  # this parameter is needed to filter by date
    }

    return params


def download_review_stats(app_id, target_timestamp=None, verbose=True):
    url = get_steam_api_url(app_id)
    params = get_request_params(target_timestamp, verbose=verbose)

    response = requests.get(url, params=params)

    if response.ok:
        result = response.json()
    else:
        result = None

    if verbose:
        print(result)

    return result

def get_input_fname():
    return "../leak/t_games_achievements_players_2018-07-01.csv"

def load_input_data(fname=None, skip_header=True):
    if fname is None:
        fname = get_input_fname()

    data = dict()

    with open(fname, "r", encoding="utf8", errors="ignore") as f:
        f_reader = csv.reader(f)

        if skip_header:
            next(f_reader)

        for row in f_reader:
            app_name = row[0]
            num_players = row[1]
            app_id = str(row[2])

            data[app_id] = dict()
            data[app_id]["name"] = app_name.strip()
            data[app_id]["sales"] = int(num_players.replace(",", ""))

    return data

def load_app_ids(data=None, fname=None, verbose=True):
    if fname is None:
        fname = get_input_fname()

    if data is None:
        data = load_input_data(fname, skip_header=True)

    app_ids = [int(app_id) for app_id in data.keys()]

    if verbose:
        print(f"#apps = {len(app_ids)}")

    return app_ids


def get_ouput_fname():
    return "./data/review_stats.json"


def load_output_dict():
    try:
        with open(get_ouput_fname(), "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        data = dict()

    return data


def write_output_dict(data):
    with open(get_ouput_fname(), "w") as f:
        json.dump(data, f)

    return


def get_rate_limits():
    # Reference: https://github.com/woctezuma/download-steam-reviews/blob/master/steamreviews/download_reviews.py

    rate_limits = {
        "max_num_queries": 600,
        "cooldown": (4 * 60) + 10,  # 4 minutes plus a cushion,
    }

    return rate_limits


def get_review_fields():
    return [
        "review_score",
        "review_score_desc",
        "total_positive",
        "total_negative",
        "total_reviews",
    ]


def download_data(app_ids, verbose=True):
    data = load_output_dict()
    processed_app_ids = [int(app_id) for app_id in data.keys()]

    unprocessed_app_ids = set(app_ids).difference(processed_app_ids)

    rate_limits = get_rate_limits()
    save_every = 15

    for query_count, app_id in enumerate(unprocessed_app_ids, start=1):
        print(f"[{query_count}/{len(unprocessed_app_ids)}] Query for appID = {app_id}")

        result = download_review_stats(app_id, verbose=verbose)

        if result is not None:
            query_summary = result["query_summary"]

            app_info = dict()
            for key in get_review_fields():
                app_info[key] = query_summary[key]

            data[app_id] = app_info
        else:
            print(f"[X] Query failed for appID = {app_id}")

        if (query_count % save_every == 0) or (query_count == len(unprocessed_app_ids)):
            write_output_dict(data)

        if query_count % rate_limits["max_num_queries"] == 0:
            write_output_dict(data)
            cooldown_duration = rate_limits["cooldown"]
            print(f"#queries {query_count} reached. Cooldown: {cooldown_duration} s")
            time.sleep(cooldown_duration)

    return data


def main():
    app_ids = load_app_ids()
    data = download_data(app_ids, verbose=False)

    return True


if __name__ == "__main__":
    import os
    os.makedirs("./data/", exist_ok=True)
    main()

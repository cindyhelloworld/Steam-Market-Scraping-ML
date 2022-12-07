import requests
import pandas as pd
import time

api_key = "6sRnydxcHkpMyGjFzXFZNZ1e6pE"
error_lst = []
result = []
coming = []

with open("leak_id.csv",'r', encoding='UTF-8') as f:
    ids = [id.strip() for id in f.readlines()[1:]]


for app_id in ids:
    page = requests.get(f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()

    while page == None:
        time.sleep(60)
        page = requests.get(f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()
    
    if page[app_id]["success"] == False:
        error_lst.append(app_id)
        continue
    
    page = page[app_id]["data"]

    keys = list(page.keys())

    if "release_date" in keys:
        dic = {}
        date = page["release_date"]
        if date["coming_soon"] == True:
            coming.append(app_id)
        else:
            str_date = date["date"]
            lst = str_date.split(" ")
            year = lst[-1]
            dic["id"] = app_id
            dic["year"] = year
            result.append(dic)
            print(dic)
    else:
        error_lst.append(app_id)

file = pd.DataFrame(result)
file.to_csv("leak_time_result.csv", encoding="utf-8", index= False)
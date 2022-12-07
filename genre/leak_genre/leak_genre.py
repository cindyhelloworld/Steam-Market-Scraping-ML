import requests
from bs4 import BeautifulSoup
import pandas as pd
import time


api_key = "6sRnydxcHkpMyGjFzXFZNZ1e6pE"
cat = {"id": [], "name": []}
gen = {"id": [], "name": []}
error_lst = []
result = []

with open("data\genre\leak_genre\leak_id.csv", "r", encoding="UTF-8") as f:
    ids = [id.strip() for id in f.readlines()[1:]]


for app_id in ids:
    dic = {}
    # page = requests.get(f"https://api.steamapis.com/market/app/{app_id}?api_key={api_key}").json()
    page = requests.get(
        f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()

    while page == None:
        print("sleep for 1 min...")
        time.sleep(60)
        page = requests.get(
            f"http://store.steampowered.com/api/appdetails?appids={app_id}").json()

    if page[app_id]["success"] == False:
        error_lst.append(app_id)
        continue

    page = page[app_id]["data"]

    keys = list(page.keys())
    if "genres" in keys and "categories" in keys:

        # get all categories of the app
        categories = page["categories"]
        cat_lst = []
        for category in categories:
            cat_id = category["id"]
            cat_name = category["description"]

            # update category_index.csv
            if cat_id not in cat["id"]:
                cat["id"].append(cat_id)
                cat["name"].append(cat_name)

            cat_lst.append(str(cat_id))

        # get all genres of the app
        genres = page["genres"]
        gen_lst = []
        for genre in genres:
            gen_id = genre["id"]
            gen_name = genre["description"]

            # upload genre_index.csv
            if gen_id not in gen["id"]:
                gen["id"].append(gen_id)
                gen["name"].append(gen_name)

            gen_lst.append(str(gen_id))

        dic["id"] = app_id
        dic["category"] = ",".join(cat_lst)
        dic["genre"] = ",".join(gen_lst)

        result.append(dic)

    elif "genres" in keys:
        genres = page["genres"]
        gen_lst = []
        for genre in genres:
            gen_id = genre["id"]
            gen_name = genre["description"]

            # upload genre_index.csv
            if gen_id not in gen["id"]:
                gen["id"].append(gen_id)
                gen["name"].append(gen_name)

            gen_lst.append(str(gen_id))

        dic["id"] = app_id
        dic["category"] = ""
        dic["genre"] = ",".join(gen_lst)

        result.append(dic)

    else:
        error_lst.append(app_id)

    print(len(result))

print("error lst length:", len(error_lst))
print("result list length:", len(result))
print("show error list:", error_lst)

final = pd.DataFrame(result)
final.to_csv("joint_category_genre.csv", index=False)
print("final result done!")

cat_df = pd.DataFrame.from_dict(cat)
cat_df.to_csv("category_index.csv", index=False)
print("category_index done!")

gen_df = pd.DataFrame.from_dict(gen)
gen_df.to_csv("genre_index.csv", index=False)
print("genre index done!")

print("done!")

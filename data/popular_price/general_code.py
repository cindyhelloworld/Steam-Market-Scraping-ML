import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0",
}

url = "https://steampricehistory.com/popular?page="
lst = []

for page in range(1, 93):  # 92 for the popularity > 500
    result = requests.get(url+str(page), headers=headers)
    soup = BeautifulSoup(result.content)
    table = soup.find("table", {"class": "app-table"})
    trs = table.find_all("tr")
    for tr in trs[1:]:
        dic = {}
        tds = tr.find_all("td")
        dic["title"] = tds[1].text.strip()
        dic["url"] = tr.find("a")["href"]
        dic["price"] = tds[2].text[1:]
        dic["discount"] = tds[3].text
        dic["popularity"] = tds[4].text
        lst.append(dic)
    time.sleep(1)

general = pd.DataFrame(lst)


def set_id(row):
    index = row.find("app/")
    return row[index+4:]


general["id"] = general["url"].apply(set_id)

general = general.set_index("id")
general.to_csv(path+"general.csv")

urls = general["url"]
path = "/Users/xxt/Desktop/steam data/"
for url in urls:
    discounts = []
    sales = []
    result = requests.get(url, headers=headers)
    soup = BeautifulSoup(result.content)
    tables = soup.find_all("section", {"class": "breakdown"})

    # only discounts and sales
    if len(tables) == 2:
        #table1: discounts
        table_discount = tables[0]
        trs = table_discount.find_all("tr")
        for tr in trs[1:]:
            dic = {}
            tds = tr.find_all("td")
            dic["date"] = tds[0].text
            dic["price"] = tds[1].text
            dic["gain"] = tds[2].text
            dic["discount"] = tds[3].text
            discounts.append(dic)

        #table2: sales
        table_sales = tables[1]
        trs = table_sales.find_all("tr")
        for tr in trs[1:]:
            dic = {}
            tds = tr.find_all("td")
            dic["sale"] = tds[0].text
            dic["date start"] = tds[1].text
            dic["price"] = tds[2].text
            dic["discount"] = tds[3].text
            sales.append(dic)

        d_file = pd.DataFrame(discounts)
        s_file = pd.DataFrame(sales)

        name = general.loc[general['url'] == url].index.item()
        d_file.to_csv(path+name+"_discounts.csv")
        s_file.to_csv(path+name+"_sales.csv")
        time.sleep(1)

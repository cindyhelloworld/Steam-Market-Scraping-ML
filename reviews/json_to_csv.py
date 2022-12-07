import pandas as pd


if __name__ == "__main__":
    df = pd.read_json("./data/review_stats.json", orient='index')
    df.index.name = "app_id"
    df.to_csv("./data/reviews_2018_all.csv")
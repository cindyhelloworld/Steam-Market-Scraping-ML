import os

with open("./finished.txt", "w", encoding="utf-8") as f:
    for s in os.listdir("./data/price_change_review"):
        f.write(s+"\n")
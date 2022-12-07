import os


target_path = "./download/"

if __name__ == "__main__":
    with open("./check.csv", "r", encoding="utf-8") as f:
        url_list = f.readlines()
    id_set = {url.split("/")[-1].strip() for url in url_list}
    print(len(id_set))
    file_set = set(os.listdir(target_path))
    download_id_set = {name.split(".")[0] for name in file_set}
    print(id_set - download_id_set)
    

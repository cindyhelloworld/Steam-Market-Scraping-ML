#
import os
import filecmp
from shutil import move


target_path = "./download/"
repeat_path = "./repeat/"

if __name__ == "__main__":
    file_set = set(os.listdir(target_path))
    checked_set = set()
    repeated_set = set()
    while file_set:
        check_name = file_set.pop()
        isRepeat = False
        for file_name in file_set:
            if (filecmp.cmp(target_path + file_name, target_path + check_name, False)):
                repeated_set.update({check_name, file_name})
                isRepeat = True
        if not isRepeat:
            checked_set.add(check_name)
        file_set = file_set - repeated_set
    for name in repeated_set:
        move(target_path + name, repeat_path + name)
    with open("./inputs/repeat_inputs.csv", "w", encoding="utf-8") as f:
        for name in repeated_set:
            id = name.split(".")[0]
            f.write(f"https://steamspy.com/app/{id}\n")
    
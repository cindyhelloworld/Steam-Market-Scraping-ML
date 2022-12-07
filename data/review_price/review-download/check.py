import os

output_set = set(os.listdir("./output"))
source_set = set(os.listdir("./source"))

diff = output_set ^ source_set
if len(output_set) != len(source_set):
    print("mismatch:", len(diff), "items")

with open("./finished.txt", 'w', encoding='utf-8') as f:
    for name in output_set:
        f.write(name+'\n')

with open("./input.txt", 'w', encoding='utf-8') as f:
    for name in diff:
        f.write(name+'\n')

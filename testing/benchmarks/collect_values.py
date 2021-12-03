import os
import sys
from re import findall

import csv

result_list = []
path = str(sys.argv[1])

for subdir, dirs, filenames in os.walk(path):
    for filename in filenames:
        if filename.endswith(".out"):
            filename_path = subdir + os.sep + filename

            filename_split = filename.split("-")

            filename_split[2] = int(filename_split[2].split(".")[0][:-1])
            filename_split[3] = int(filename_split[3].split(".")[0][:-1])
            filename_split[4] = int(filename_split[4].split(".")[0][:-1])

            with  open(filename_path, 'r') as filename:
                data = filename.read()
                values = findall(r"Time taken: (\d*\.?\d+) seconds", data)
                times = [float(item) for item in values]

                if len(times) != 1:
                    pass
                    # raise ValueError("There must be exactly one time per filename!")
                else:
                    result_list.append((filename_split[0], filename_split[1], "skyline", filename_split[4], filename_split[3], filename_split[2], times[0]))

with open(path + "/_output.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerows([["algorithm", "dataset", "query", "nodes", "dimensions", "size", "time"]])
    writer.writerows(result_list)

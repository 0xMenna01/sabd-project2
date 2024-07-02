import json
import os
import csv

RESULTS_PATH = os.getenv("RESULTS_PATH", "/results")


def init_env():
    # delete all contents in the results folder
    os.system(f"rm -rf {RESULTS_PATH}/*")


def write_csv_from(topic_name: str, row: tuple) -> None:

    if topic_name.startswith("query1"):
        header = ["ts", "vault id", "count", "mean_s194", "stddev_s194"]
    elif topic_name.startswith("query2"):
        pass
    else:
        raise ValueError(f"Unknown topic name: {topic_name}")

    # write to csv and handle file exist or not
    file_path = f"{RESULTS_PATH}/{topic_name}.csv"
    file_exists = os.path.isfile(file_path)
    with open(file_path, "a", newline="") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)

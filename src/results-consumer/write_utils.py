import json
import os
import csv
from typing import List, Tuple

RESULTS_PATH = os.getenv("RESULTS_PATH", "/results")

HEADER_QUERY1 = ["ts", "vault_id", "count", "mean_s194", "stddev_s194"]

HEADER_QUERY2 = [
    "ts",
    "vault_id1",
    "failures1",
    "vault_id2",
    "failures2",
    "vault_id3",
    "failures3",
    "vault_id4",
    "failures4",
    "vault_id5",
    "failures5",
    "vault_id6",
    "failures6",
    "vault_id7",
    "failures7",
    "vault_id8",
    "failures8",
    "vault_id9",
    "failures9",
    "vault_id10",
    "failures10",
]


def init_env():
    # delete all contents in the results folder
    os.system(f"rm -rf {RESULTS_PATH}/*")


def write_csv_from(topic_name: str, row: Tuple) -> None:

    if topic_name.startswith("query1"):
        header = HEADER_QUERY1
    elif topic_name.startswith("query2"):
        header = HEADER_QUERY2
        # Before conversion, row is (ts, List[(vault_id, failures_count, list_of_models_and_serial_numbers)])
        row = (row[0],) + tuple(convert_ranking_for_csv(row[1]))
    else:
        raise ValueError(f"Unknown topic name: {topic_name}")

    # Write to csv and handle if file exists or not
    file_path = f"{RESULTS_PATH}/{topic_name}.csv"
    file_exists = os.path.isfile(file_path)
    with open(file_path, "a", newline="") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(header)
        writer.writerow(row)


# Utility function for writing ranking results of query 2 to a CSV file
def convert_ranking_for_csv(rank_list: List[Tuple[int, int, List[str]]]) -> List[str]:
    """Converts the List of [Tuple[vault_id, failures_count, list_models_and_serials]] to a unique List of strings: ["vault_id1", "failures1 (model1, serial_number1, ...)", ..., "vault_id10", "failures10 (model10, serial_number10, ...)"]"""

    res = []
    for vault_id, failures_count, disks in rank_list:
        res.append(str(vault_id))
        disks = ", ".join([f"{disk[0]}, {disk[1]}" for disk in disks])
        res.append(f"{str(failures_count)} ({disks})")

    if len(res) < 10 * 2:
        # append null values
        res += [""] * (10 * 2 - len(res))

    return res

import os
import zipfile
import json


def extract_gps_data():
    os.makedirs("data-publisher/input_data", exist_ok=True)
    with zipfile.ZipFile("data-publisher/data.zip", "r") as zip_ref:
        zip_ref.extractall("data-publisher/input_data")


if __name__ == "__main__":
    extract_gps_data()

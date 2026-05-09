import os
import zipfile
import json
from decorators import clean_folder

TMP_FOLDER = "data-publisher/input_data"


# @clean_folder(TMP_FOLDER)
def extract_gps_data():
    os.makedirs(TMP_FOLDER, exist_ok=True)
    with zipfile.ZipFile("data-publisher/data.zip", "r") as zip_ref:
        zip_ref.extractall(TMP_FOLDER)


if __name__ == "__main__":
    extract_gps_data()

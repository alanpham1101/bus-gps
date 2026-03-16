import os
import zipfile
import pandas as pd
import folium
import json

from decorators import clean_folder

TMP_FOLDER = "data-publisher/data-extracted"
VEHICLE_ID = "22172956b07d3bab7113aa8d74ab7c3921072273673404fed97ac7dd7d1c8d3a"


@clean_folder(TMP_FOLDER)
def extract_gps_data():
    os.makedirs(TMP_FOLDER, exist_ok=True)
    with zipfile.ZipFile("data-publisher/data.zip", "r") as zip_ref:
        zip_ref.extractall(TMP_FOLDER)


def plot_gps_bus_data():
    with open('data-publisher/data-extracted/sample.json', 'r') as file:
        data = json.load(file)

    map = folium.Map(locations=[10.723508322083333, 106.65587462609874], zoom_start=20)
    df = pd.json_normalize(data, sep='_')
    filtered_df = df[df["msgBusWayPoint_vehicle"] == VEHICLE_ID]

    for _, row in filtered_df.iterrows():
        folium.CircleMarker(
            [row["msgBusWayPoint_y"], row["msgBusWayPoint_x"]],
            radius=2, color='red', fill=True
        ).add_to(map)
    map.save('data-publisher/data-extracted/sample_map.html')


if __name__ == "__main__":
    extract_gps_data()
    plot_gps_bus_data()

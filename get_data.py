# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 16:58:18 2025

@author: DiMartino
"""
import requests
import pandas as pd
import os

def get_single_metadata(get_url, csv_output):
    pass

def get_data(get_url, parquet_output):
    headers = {"Accept": "application/vnd.sdmx.data+csv;version=1.0.0"}
    response = requests.get(get_url, headers=headers, verify=False)
    if response.status_code == 200:
        with open("temp_csv.csv", "wb") as file:
            file.write(response.content)
    else:
        print("La richiesta non è andata a buon fine.")
    df = pd.read_csv("temp_csv.csv")
    os.remove("temp_csv.csv")
    df = df.dropna(axis=1, how="all")
    columns_to_keep = ["OBS_VALUE", "TIME_PERIOD", "DATAFLOW", "CLASSE_ETA"]
    for column in df.columns:
        if column not in columns_to_keep:
            df = df.drop(columns=column)
    df.to_parquet(parquet_output)
    print(df)
    print(df.columns)

url = "http://sdmx.istat.it/SDMXWS/rest/data/6_64/............/"
base_url = "http://sdmx.istat.it/SDMXWS/rest/data/"

url_list = [
    "151_874/M.Y...9..2025M1G7/?startPeriod=2024",
    ""
    ]



# url = "http://sdmx.istat.it/SDMXWS/rest/data/151_874/M.Y...9..2025M1G7/?startPeriod=2024"
parquet = "try_disoccupazione.parquet"
get_data(url, parquet)
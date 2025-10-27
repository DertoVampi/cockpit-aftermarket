# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 14:49:12 2025

@author: DiMartino
"""
import duckdb
import pandas as pd
import numpy as np
import time
from sqlalchemy import create_engine, text
import datetime as dt
import os
from ecbdata import ecbdata
import urllib
import imfp
import shutil
from pyistat import get


def login_row(lines, lookup_string):  # Create config.txt to store secrets.
    for line in lines:
        if lookup_string in line:
            target_line = line.strip()
            break
    else:
        return None
    cleaned_line = target_line.strip().split(" ")[1]
    return cleaned_line


def get_login_info_from_config(config_file):  # Get login info from config.
    """

    Returns variables used for logins.
    -------
    How to use it:
    remember that in case you do not need to call all the variables, you can call the variables using
    only the needed variables and adding *rest for the others. Example: if you only need
    user and password, you can call user, password, *rest = get_login_info_from_config().

    """
    # if not os.path.exists(config_file):
    #     build_config()
    with open(config_file, "r") as file:
        lines = file.readlines()
        username = login_row(lines, "username:").strip()
        password = login_row(lines, "password:").strip()
        server = login_row(lines, "server:").strip()
        database = login_row(lines, "database:").strip()
        ftp_server_address = login_row(lines, "ftp_server_address:").strip()
        ftp_user = login_row(lines, "ftp_user:").strip()
        ftp_password = login_row(lines, "ftp_password:").strip()
        tenant_id = login_row(lines, "tenant_id:").strip()
        app_id = login_row(lines, "app_id:").strip()
        secret = login_row(lines, "random_id:").strip()
        return (
            username,
            password,
            server,
            database,
            ftp_server_address,
            ftp_user,
            ftp_password,
            tenant_id,
            app_id,
            secret,
        )


def build_connection_string(config_file):
    (
        username,
        password,
        server,
        database,
        ftp_server_address,
        ftp_user,
        ftp_password,
        tenant_id,
        app_id,
        secret,
    ) = get_login_info_from_config(config_file)
    app_id_encoded = urllib.parse.quote_plus(app_id)
    secret_encoded = urllib.parse.quote_plus(secret)
    connection_string = (
        f"mssql+pyodbc://{app_id_encoded}:{secret_encoded}@"
        f"{server}/{database}?"
        f"driver=ODBC+Driver+17+for+SQL+Server&"
        f"Authentication=ActiveDirectoryServicePrincipal&"
        f"TenantID={tenant_id}&"
        f"Encrypt=yes&"
        f"TrustServerCertificate=no"
    )
    return connection_string


# The query extracts from ANFIA's SQL Server database
query = """
DECLARE @end_period AS DATE = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
DECLARE @start_period DATE = CASE
                                WHEN MONTH(GETDATE()) > 3 THEN DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE())-3, 1)
                                ELSE DATEFROMPARTS(YEAR(GETDATE())-1, MONTH(GETDATE())+9, 1)
                             END;
WITH pesanti AS (SELECT
    mercato.desMercato AS 'CATEGORIA_VEICOLO',  
    ISNULL(alimentazione.desAlimentazione, 'ND') AS 'ALIMENTAZIONE', 
    imm.data_immatricolazione_del_veicolo AS 'DATA_IMMATRICOLAZIONE',
    COUNT(imm.numero_targa) AS 'CONTEGGIO'
FROM Immatricolazioni imm
INNER JOIN Omologazioni omm
	ON omm.codice_omologazione = imm.omologazione_cuc
LEFT JOIN Mercato mercato
	ON imm.codMercato = mercato.codMercato
LEFT JOIN Alimentazione alimentazione
	ON omm.codCombustibile = alimentazione.codAlimentazione
LEFT JOIN dbo.TargheAnnullateReporting AnnullamentiReporting 
	ON AnnullamentiReporting.immatricolazioniId = imm.immatricolazioniId
WHERE
	flNuovo = 1
	AND imm.numero_targa IS NOT NULL
	AND omm.codice_omologazione IS NOT NULL
	AND CASE WHEN AnnullamentiReporting.immatricolazioniId IS NOT NULL and flannullato = 1 THEN 0 ELSE flannullato END = 0
	AND imm.data_libretto_del_veicolo >= '2023-01-01'
	AND imm.data_libretto_del_veicolo < @end_period
	AND mercato.codMercato IN ('01', '02', '04')
GROUP BY
    mercato.desMercato,
    alimentazione.desAlimentazione,
    imm.data_immatricolazione_del_veicolo
    ),
leggeri AS (SELECT
        mercato.desMercato AS 'CATEGORIA_VEICOLO',  
        ISNULL(alimentazione.desAlimentazione, 'ND') AS 'ALIMENTAZIONE', 
        imm.data_immatricolazione_del_veicolo AS 'DATA_IMMATRICOLAZIONE',
        COUNT(imm.numero_targa) AS 'CONTEGGIO'
FROM dbo.ImmatricolazioniVeicoliLeggeri imm 
INNER JOIN dbo.Omologazioni omm ON omm.codice_omologazione = imm.omologazione_cuc 
LEFT JOIN Mercato mercato
	ON imm.codMercato = mercato.codMercato
LEFT JOIN Alimentazione alimentazione
	ON omm.codAlimentazione = alimentazione.codAlimentazione
LEFT JOIN dbo.TargheAutoAnnullateReporting AnnullamentiReporting 
	ON AnnullamentiReporting.immatricolazioniId = imm.immatricolazioniId
WHERE
    imm.data_immatricolazione_del_veicolo >= '2023-01-01'
    AND imm.data_immatricolazione_del_veicolo < @end_period
    AND imm.numero_targa IS NOT NULL
    AND CASE WHEN AnnullamentiReporting.immatricolazioniId IS NOT NULL and flannullato = 1 THEN 0 ELSE flannullato END = 0
    AND imm.flNuovo = 1 
    AND omm.codice_omologazione IS NOT NULL
GROUP BY 
    mercato.desMercato,
    alimentazione.desAlimentazione,
    imm.data_immatricolazione_del_veicolo
    )
(SELECT 
    CATEGORIA_VEICOLO,  
    ALIMENTAZIONE,
    DATA_IMMATRICOLAZIONE,
    CONTEGGIO,
    FORMAT(DATA_IMMATRICOLAZIONE, 'yyyyMMdd') AS idData
FROM pesanti)
UNION ALL 
(SELECT
    CATEGORIA_VEICOLO,  
    ALIMENTAZIONE,
    DATA_IMMATRICOLAZIONE,
    CONTEGGIO,
    FORMAT(DATA_IMMATRICOLAZIONE, 'yyyyMMdd') AS idData
FROM leggeri)
"""


config_file = r"C:\Users\dimartino\AppData\Local\pysecrets\pyconfig.txt"
connection_string = build_connection_string(config_file)

# Creazione della stringa di connessione per SQLAlchemy

current_month = dt.date.today().month
focus_year = (
    dt.date.today().year if current_month != 1 else dt.date.today().year - 1
)
focus_month = dt.date.today().month - 1 if current_month != 1 else 12


# PIL
pil_df = get.get_data(
    "163_156_DF_DCCN_SQCQ_3",
    dimensions=["Q", "IT", "", "L_2020", "N", ""],
    force_url=True,
    timeout=120,
)
# Crescita PIL
pil_growth_df = get.get_data(
    "163_156_DF_DCCN_SQCQ_2",
    dimensions=["Q", "IT", "B1GQ_B_W2_S1", "GO1", "Y", ""],
    force_url=True,
    timeout=120,
)
# Disoccupazione
unemployment_df = get.get_data(
    "151_874_DF_DCCV_TAXDISOCCUMENS1_1",
    dimensions=["", "", "", "N", "9", "Y15-74", ""],
    force_url=True,
    timeout=120,
)
# Inflazione
inflation_df = get.get_data(
    "167_742",
    dimensions=["A", "IT", "", "4", "00"],
    force_url=True,
    timeout=120,
)
# Clima di fiducia delle imprese
trust_df = get.get_data(
    "6_64_DF_DCSC_IESI_2",
    dimensions=["", "", "", "", ""],
    force_url=True,
    timeout=120,
)
# Tassi d'interesse BCE
interest_rates_df = ecbdata.get_series("FM.B.U2.EUR.4F.KR.MRR_FR.LEV")[
    ["TITLE", "TIME_PERIOD", "OBS_VALUE"]
]
# Prezzi materie prime IMF - TEMPORANEAMENTE GESTITO CON CSV SCARICATO DAL SITO IMF - DATASET PCPS
# focus_commodities = ["pcopp", "pgaso", "pgold", "psilver", "poilapsp", "piorecr", "plith", "palum", "pcoba", "preodom"]
# focus_commodities = [x.upper() for x in focus_commodities]
# imfp.set_imf_app_name("ANFIA")
# commodity_df = imfp.imf_dataset(
#     database_id="PCPS",
#     freq=["M"],
#     unit_measure = "USD",
#     start_year=2020,
#     commodity="palum",
#     end_year=focus_year)
# commodity_df = commodity_df[commodity_df["commodity"].isin(focus_commodities)]
csv_file = r"C:\Users\dimartino\Downloads\dataset_2025-10-24T13_17_03.009871102Z_DEFAULT_INTEGRATION_IMF.RES_PCPS_9.0.0.csv"
commodity_df = pd.read_csv(csv_file)
# Costi energia
fuel_energy_df = get.get_data(
    "168_760_DF_DCSP_IPCA1B2015_2",
    dimensions=["", "", "", "4", "ENRGY_5DG"],
    force_url=True,
    timeout=120,
)
energy_df = get.get_data(
    "168_760_DF_DCSP_IPCA1B2015_2",
    dimensions=["", "", "", "4", "FUELS_5DG"],
    force_url=True,
    timeout=120,
)


event_dict = {
    "PIL": [1, 0],
    "CrescitaPIL": [1, 1],
    "Disoccupazione": [1, 2],
    "Inflazione": [1, 3],
    "FiduciaImprese": [1, 4],
    "TassiDiInteresse": [2, 5],
    "BeniEnergetici": [1, 6],
    "CarburanteTrasporti": [1, 7],
}

event_dict = {
    "PIL": 1,
    "CrescitaPIL": 2,
    "Disoccupazione": 3,
    "Inflazione": 4,
    "FiduciaImprese": 5,
    "TassiDiInteresse": 6,
    "BeniEnergetici": 7,
    "CarburanteTrasporti": 8,
    "PrezzoOro": 154,
    "PrezzoRame": 155,
    "PrezzoArgento": 156,
    "PrezzoGasolio": 157,
    "PrezzoPetrolio": 158,
    "PrezzoAlluminio": 187,
    "PrezzoGasNaturale": 188,
    "PrezzoFerroGrezzo": 189,
    "PrezzoLitio": 190,
    "PrezzoCobalto": 191,
    "PrezzoTerreRare": 192,
    "TotaleIndiceWCI": 266,
    "RotterdamIndiceWCI": 267,
    "LosAngelesIndiceWCI": 268,
    "GenovaIndiceWCI": 269,
    "NewYorkIndiceWCI": 270,
}

ifm_mapping = {
    "PCOPP": "PrezzoRame",
    "PGASO": "PrezzoGasolio",
    "PGOLD": "PrezzoOro",
    "PSILVER": "PrezzoArgento",
    "POILAPSP": "PrezzoPetrolio",
    "PALUM": "PrezzoAlluminio",
    "PNGAS": "PrezzoGasNaturale",
    "PREODOM": "PrezzoTerreRare",
    "PCOBA": "PrezzoCobalto",
    "PLITH": "PrezzoLitio",
    "PIORECR": "PrezzoFerroGrezzo",
}

quarter_mapping = {"Q1": "03", "Q2": "06", "Q3": "09", "Q4": "12"}

fuel_mapping = {
    "BENZINA": 0,
    "HEV (B)": 1,
    "B/OLIO": 2,
    "B/WANK": 3,
    "BENZINA-ETANOLO": 4,
    "HEV (B-GPL)": 5,
    "BEV": 6,
    "DIESEL": 7,
    "HEV (D)": 8,
    "BIODIESEL": 9,
    "GNL": 10,
    "GPL": 11,
    "IDROGENO": 12,
    "IBRIDO METANO/ELETTRICO": 13,
    "METANO": 14,
    "MISCELA": 15,
    "PETROLIO": 16,
    "PHEV": 17,
    "PHEV IDROGENO": 18,
    "ND": 99,
}

type_mapping = {
    "AUTOBUS": "01",
    "AUTOCARRI": "02",
    "RIMORCHI LEGGERI": "03",
    "RIMORCHI PESANTI": "04",
    "SEMIRIMORCHI": "05",
    "VEICOLI COMMERCIALI LEGGERI": "06",
    "AUTOVETTURE": "07",
    "SCONOSCIUTO": "99",
}

month_mapping = {
    "01": "Q1",
    "02": "Q1",
    "03": "Q1",
    "04": "Q2",
    "05": "Q2",
    "06": "Q2",
    "07": "Q3",
    "08": "Q3",
    "09": "Q3",
    "10": "Q4",
    "11": "Q4",
    "12": "Q4",
}

iam_price_mapping = {
    "Delta % Prezzo Medio Puntuale": "9",
    "Delta % Prezzo Medio YTD": "10",
    "Delta % Prezzo Medio Rolling": "11",
}

iam_rolling_mapping = {
    "Delta Fatturato": 12,
    "Effetto Volumi": 13,
    "Effetto Mix CP": 14,
    "Effetto Prezzi": 15,
}

iam_puntuale_mapping = {
    "Delta Fatturato": 16,
    "Effetto Volumi": 17,
    "Effetto Mix CP": 18,
    "Effetto Prezzi": 19,
}

iam_progressivo_mapping = {
    "Delta Fatturato": 20,
    "Effetto Volumi": 21,
    "Effetto Mix CP": 22,
    "Effetto Prezzi": 23,
}

month_name_mapping = {
    "gennaio": "01",
    "febbraio": "02",
    "marzo": "03",
    "aprile": "04",
    "maggio": "05",
    "giugno": "06",
    "luglio": "07",
    "agosto": "08",
    "settembre": "09",
    "ottobre": "10",
    "novembre": "11",
    "dicembre": "12",
}

month_number_mapping = {
    1: "gennaio",
    2: "febbraio",
    3: "marzo",
    4: "aprile",
    5: "maggio",
    6: "giugno",
    7: "luglio",
    8: "agosto",
    9: "settembre",
    10: "ottobre",
    11: "novembre",
    12: "dicembre",
}


def extract_date(df):
    df["idData"] = ""
    for index, row in df.iterrows():
        date = str(row["TIME_PERIOD"])
        if "q" in date.casefold():
            year = date[:4]
            quarter = date[-1]
            month = (
                "01"
                if quarter == "1"
                else (
                    "04"
                    if quarter == "2"
                    else "07" if quarter == "3" else "10"
                )
            )
            iddata = str(year) + str(month) + "01"
            df.at[index, "idData"] = iddata
        elif len(date) == 4:
            year = str(date)
            iddata = year + "0101"
            df.at[index, "idData"] = iddata
        else:
            year = date[:4]
            month = date[-2:]
            iddata = str(year) + str(month) + "01"
            df.at[index, "idData"] = iddata
    return df


def prepare_df_api(df):
    prepped_df = pd.DataFrame(
        columns=["Evento", "Valore", "idData", "NomePipeline"]
    )
    df = extract_date(df)
    prepped_df["idData"] = df["idData"]
    prepped_df["Valore"] = df["OBS_VALUE"]
    return prepped_df


def assign_id(df, event_mapping=event_dict):
    prepped_df = df
    prepped_df["idDato"] = df["Evento"].map(event_mapping)
    return prepped_df


def eu_manipulate_df(df, event=event_dict, mapping=month_mapping):
    df.columns = df.columns.str.strip()
    df["TITLE"] = df["TITLE"].str.strip()
    df["TITLE"] = df["TITLE"].replace(
        "Main refinancing operations - fixed rate tenders (fixed rate) (date of changes) - Level",
        "TassiDiInteresse",
    )
    df = df.rename(
        columns={
            "TITLE": "Evento",
            "TIME_PERIOD": "Data",
            "OBS_VALUE": "Valore",
        }
    )
    df[["Anno", "Mese", "Giorno"]] = df["Data"].str.split("-", expand=True)
    df["idData"] = (
        df["Anno"].astype(str)
        + df["Mese"].astype(str)
        + df["Giorno"].astype(str)
    )
    df["Quarter"] = df["Mese"].map(mapping).astype(str)
    df["Valore"] = df["Valore"].astype(float)
    df["nomePipeline"] = "BCE"
    return df


def manipulate_energy_df(df):
    if df is m_energy_df:
        fuel_df = df[df["Evento"] == "CarburanteTrasporti"]
        price_df = df[df["Evento"] == "BeniEnergetici"]
    else:
        print("Attenzione, caricato df sbagliato. Controlla.")
        return None, None
    return price_df, fuel_df


def extract_iam(excel_file_path, sheet_name, skiprows):
    if os.path.exists(excel_file_path):
        df = pd.read_excel(excel_file_path, sheet_name, skiprows=skiprows)
    else:
        print(
            "Dati IAM del mese scorso non trovati. Continuo senza aggiornare i dati IAM."
        )
        df = None
    return df


def manipulate_ifm_df(df):
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns={"indicator.id": "commodity"})  # Temporary with CSV
    df = df.rename(columns={"time_period": "Data", "obs_value": "Valore"})
    df[["Anno", "Mese"]] = df["Data"].str.split(
        "-M", expand=True
    )  # Care for this SPLIT, on the CSV date was 2015-M01
    df["idData"] = df["Anno"].astype(str) + df["Mese"].astype(str) + "01"
    df["Valore"] = round(df["Valore"].astype(float), 2)
    df["Evento"] = df["commodity"].map(ifm_mapping).astype(str)
    return df


def extract_wci(excel_file_path, skiprows=1):
    if os.path.exists(excel_file_path):
        df = pd.read_excel(excel_file_path, skiprows=skiprows)
    else:
        print(
            "Attenzione: dati del WCI non trovati. Probabilmente sono stati spostati."
        )
        df = None
    return df


focus_month_name = month_number_mapping[focus_month].capitalize()
excel_file_path = rf"L:\01.Dati\04.Varie\08.Cockpit\csv_database\IAM\Report IAM Trend Italia Distributori IAM- {focus_month_name} {focus_year}.xlsx"
wci_excel_file_path = r"L:\01.Dati\04.Varie\08.Cockpit\files\Drewry_WCI.xlsx"
m_pil_df = prepare_df_api(pil_df)
m_pil_growth_df = prepare_df_api(pil_growth_df)
m_unemployment_df = prepare_df_api(unemployment_df)
m_inflation_df = prepare_df_api(inflation_df)
m_trust_df = prepare_df_api(trust_df)
m_fuel_energy_df = prepare_df_api(fuel_energy_df)
m_energy_df = prepare_df_api(energy_df)
m_interest_rates_df = eu_manipulate_df(interest_rates_df)
m_iam_price_df = extract_iam(excel_file_path, "Prezzi Medi", skiprows=5)
m_iam_car_df = extract_iam(
    excel_file_path, "Auto-Rolling MERCATO", skiprows=35
)
m_iam_truck_df = extract_iam(
    excel_file_path, "Truck-Rolling MERCATO", skiprows=35
)
m_iam_puntuale_car_df = extract_iam(
    excel_file_path, "Auto-Puntuale MERCATO", skiprows=34
)
m_iam_puntuale_truck_df = extract_iam(
    excel_file_path, "Truck-Puntuale MERCATO", skiprows=34
)
m_iam_progressivo_car_df = extract_iam(
    excel_file_path, "Auto-Progressivo MERCATO", skiprows=33
)
m_iam_progressivo_truck_df = extract_iam(
    excel_file_path, "Truck-Progressivo MERCATO", skiprows=33
)
m_commodity_df = manipulate_ifm_df(commodity_df)
m_wci_df = extract_wci(wci_excel_file_path)

try:
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        m_reg_df = pd.DataFrame(result.fetchall(), columns=result.keys())
except Exception as e:
    print(e)


def prepare_iam_price_df(df, iam_mapping=iam_price_mapping):
    if df is None:
        return df
    focus_month = (
        dt.date.today().month - 2 if dt.date.today().month != 1 else 12
    )
    focus_year = (
        dt.date.today().year
        if dt.date.today().month != 1
        else dt.date.today().year - 1
    )
    df = df.drop(
        [
            "Unnamed: 0",
            "Unnamed: 3",
            "Unnamed: 2",
            f"{focus_month_name} {focus_year}",
        ],
        axis=1,
    )
    df = df.iloc[:2]
    df["idData"] = str(focus_year) + str(focus_month).zfill(2) + "01"
    # df["idData"] = "20250601"
    df["idTipoVeicolo"] = range(len(df))
    df = df.melt(
        id_vars=["idData", "idTipoVeicolo"],
        value_vars=[
            "Delta % Prezzo Medio Puntuale",
            "Delta % Prezzo Medio YTD",
            "Delta % Prezzo Medio Rolling",
        ],
        var_name="nomeEvento",
        value_name="Valore",
    )
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idPipeline"] = 0
    return df


def prepare_iam_df(
    df, vehicle_type, iam_mapping, month_name_mapping=month_name_mapping
):
    if df is None:
        return df
    df = df.drop(["Unnamed: 0", "Unnamed: 1", "Unnamed: 15"], axis=1)
    df = df.rename(columns={"Unnamed: 2": "nomeEvento"})

    df["nomeEvento"] = df["nomeEvento"].replace(
        "∆ Fatturato", "Delta Fatturato"
    )
    df_columns = df.columns.tolist()
    ordered_cols = df_columns[1:13]
    df = df.melt(
        id_vars="nomeEvento",
        value_vars=ordered_cols,
        var_name="Mese",
        value_name="Valore",
    )
    df["numMese"] = df["Mese"].str.casefold().map(month_name_mapping)
    january_index = df[df["numMese"] == "01"].index[0]
    df = df[df["Mese"] != "Unnamed: 3"]
    df["idPipeline"] = 0
    df["Anno"] = [
        dt.date.today().year - 1 if i < january_index else dt.date.today().year
        for i in range(len(df))
    ]
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idData"] = df["Anno"].astype(str) + df["numMese"].astype(str) + "01"
    if vehicle_type.casefold() == "car":
        df["idTipoVeicolo"] = 0
    elif vehicle_type.casefold() == "truck":
        df["idTipoVeicolo"] = 1
    return df


def prepare_iam_df_prog(
    df, vehicle_type, iam_mapping, month_name_mapping=month_name_mapping
):
    if df is None:
        return df
    df = df.drop(["Unnamed: 0", "Unnamed: 1", "Progressivo"], axis=1)
    df = df.rename(columns={"Unnamed: 2": "nomeEvento"})
    df["nomeEvento"] = df["nomeEvento"].replace(
        "∆ Fatturato", "Delta Fatturato"
    )
    df_columns = df.columns.tolist()
    ordered_cols = df_columns[1:13]
    df = df.melt(
        id_vars="nomeEvento",
        value_vars=ordered_cols,
        var_name="Mese",
        value_name="Valore",
    )
    df["numMese"] = df["Mese"].str.casefold().map(month_name_mapping)
    df = df[df["Mese"] != "Unnamed: 3"]
    df = df[~pd.isna(df["Valore"])]
    df["idPipeline"] = 0
    df["Anno"] = (
        dt.date.today().year - 1
        if dt.date.today().month == 1
        else dt.date.today().year
    )
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idData"] = df["Anno"].astype(str) + df["numMese"].astype(str) + "01"
    if vehicle_type.casefold() == "car":
        df["idTipoVeicolo"] = 0
    elif vehicle_type.casefold() == "truck":
        df["idTipoVeicolo"] = 1
    return df


def prepare_registration_df(
    df, fuel_mapping=fuel_mapping, type_mapping=type_mapping
):
    if df is None:
        return df
    df = df.drop(["DATA_IMMATRICOLAZIONE"], axis=1)
    df = df.rename(
        columns={
            "CATEGORIA_VEICOLO": "nomeMercato",
            "ALIMENTAZIONE": "nomeAlimentazione",
        }
    )
    df["nomeMercato"] = df["nomeMercato"].fillna("SCONOSCIUTO")
    df["idMercato"] = df["nomeMercato"].map(type_mapping)
    df["idAlimentazione"] = df["nomeAlimentazione"].map(fuel_mapping)
    df = df[df["idData"] > "19900101"]
    return df


def prepare_df(df, event_mapping=event_dict):
    if df is None:
        return df
    df_event = df["Evento"].iloc[0]
    prepped_df = df[["idData", "Valore"]].copy()
    prepped_df["idDato"] = event_mapping[df_event]
    return prepped_df


def prepare_ifm_df(df, event_mapping=event_dict):
    if df is None:
        return df
    prepped_df = df[["idData", "Valore", "Evento"]].copy()
    prepped_df["idDato"] = df["Evento"].map(event_mapping)
    return prepped_df


def prepare_wci_df(df, wci_mapping=event_dict):
    df = df.melt(
        id_vars=["idData"],
        value_vars=[
            "TotaleIndiceWCI",
            "RotterdamIndiceWCI",
            "LosAngelesIndiceWCI",
            "GenovaIndiceWCI",
            "NewYorkIndiceWCI",
        ],
        var_name="nomeEvento",
        value_name="Valore",
    )
    df["idDato"] = df["nomeEvento"].map(wci_mapping)
    return df


m_pil_df["Evento"] = "PIL"
prepped_pil_df = assign_id(m_pil_df)
m_inflation_df["Evento"] = "Inflazione"
prepped_inflation_df = assign_id(m_inflation_df)
m_energy_df["Evento"] = "BeniEnergetici"
prepped_energy_price_df = assign_id(m_energy_df)
m_fuel_energy_df["Evento"] = "CarburanteTrasporti"
prepped_energy_fuel_df = assign_id(m_fuel_energy_df)
m_trust_df["Evento"] = "FiduciaImprese"
prepped_trust_df = assign_id(m_trust_df)
m_pil_growth_df["Evento"] = "CrescitaPIL"
prepped_pil_growth_df = assign_id(m_pil_growth_df)
m_unemployment_df["Evento"] = "Disoccupazione"
prepped_unemployment_df = assign_id(m_unemployment_df)
prepped_interest_rates_df = prepare_df(m_interest_rates_df)

prepped_iam_price_df = prepare_iam_price_df(m_iam_price_df)
prepped_iam_car_df = prepare_iam_df(
    m_iam_car_df, vehicle_type="car", iam_mapping=iam_rolling_mapping
)
prepped_iam_truck_df = prepare_iam_df(
    m_iam_truck_df, vehicle_type="truck", iam_mapping=iam_rolling_mapping
)
prepped_iam_puntuale_car_df = prepare_iam_df_prog(
    m_iam_puntuale_car_df, vehicle_type="car", iam_mapping=iam_puntuale_mapping
)
prepped_iam_puntuale_truck_df = prepare_iam_df_prog(
    m_iam_puntuale_truck_df,
    vehicle_type="truck",
    iam_mapping=iam_puntuale_mapping,
)
prepped_iam_progressivo_car_df = prepare_iam_df_prog(
    m_iam_progressivo_car_df,
    vehicle_type="car",
    iam_mapping=iam_progressivo_mapping,
)
prepped_iam_progressivo_truck_df = prepare_iam_df_prog(
    m_iam_progressivo_truck_df,
    vehicle_type="truck",
    iam_mapping=iam_progressivo_mapping,
)

prepped_reg_df = prepare_registration_df(m_reg_df)
prepped_commodity_df = prepare_ifm_df(m_commodity_df)
prepped_wci_df = prepare_wci_df(m_wci_df)

df_list = [
    prepped_pil_df,
    prepped_pil_growth_df,
    prepped_unemployment_df,
    prepped_inflation_df,
    prepped_trust_df,
    prepped_interest_rates_df,
    prepped_energy_price_df,
    prepped_energy_fuel_df,
    prepped_commodity_df,
]

iam_df_list = [
    prepped_iam_price_df,
    prepped_iam_car_df,
    prepped_iam_truck_df,
    prepped_iam_puntuale_car_df,
    prepped_iam_puntuale_truck_df,
    prepped_iam_progressivo_car_df,
    prepped_iam_progressivo_truck_df,
]

conn = duckdb.connect(
    r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db"
)

for df in df_list:
    if df is not None:
        for index, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO Dati (idDato, idData, valore, latest)
                VALUES (?, ?, ?, 0)
                ON CONFLICT (idDato, idData, idTipoVeicolo)
                DO UPDATE SET valore = EXCLUDED.valore
                """,
                (row["idDato"], row["idData"], row["Valore"]),
            )

if prepped_wci_df is not None:
    for index, row in prepped_wci_df.iterrows():
        conn.execute(
            """
                     INSERT INTO Dati (idDato, idData, valore, idTipoVeicolo, latest)
                     VALUES (?, ?, ?, 2, 0)
                     ON CONFLICT (idDato, idData, idTipoVeicolo)
                     DO UPDATE SET valore = EXCLUDED.valore
                     """,
            (row["idDato"], row["idData"], row["Valore"]),
        )

for df in iam_df_list:
    if df is not None:
        for index, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO Dati (idDato, idData, idTipoVeicolo, valore, latest)
                VALUES(?,?,?,?,0)
                ON CONFLICT(idDato, idData, idTipoVeicolo)
                DO UPDATE SET valore = EXCLUDED.valore
                """,
                (
                    row["idDato"],
                    row["idData"],
                    row["idTipoVeicolo"],
                    row["Valore"],
                ),
            )

conn.execute(
    """
    WITH latestData AS (
        SELECT
        idDato,
        idTipoVeicolo,
        MAX(idData) as idDataMAX
        FROM Dati 
        GROUP BY idDato, idTipoVeicolo
        )
    UPDATE Dati
    SET latest = CASE
                    WHEN(idDato, idTipoVeicolo, idData) IN (SELECT idDato, idTipoVeicolo, idDataMAX FROM LatestData) THEN 1
                    ELSE 0
                END;
    """
)

for index, row in prepped_reg_df.iterrows():
    conn.execute(
        """
        INSERT INTO ImmDati (idData, idAlimentazione, idMercato, valore, latest)
        VALUES(?,?,?,?,0)
        ON CONFLICT(idData, idAlimentazione, idMercato)
        DO UPDATE SET valore = EXCLUDED.valore
        """,
        (
            row["idData"],
            row["idAlimentazione"],
            row["idMercato"],
            row["CONTEGGIO"],
        ),
    )

conn.execute(
    """
    WITH latestYear AS (
        SELECT
        MAX(anno) as annoMAX
        FROM ImmDati
        INNER JOIN dimension_Data data
            ON data.idData = ImmDati.idData
        ),
    latestData AS (
        SELECT
        MAX(mese) as meseMAX,
        MAX(anno) as annoMAX
        FROM ImmDati
        INNER JOIN dimension_Data data
            ON data.idData = ImmDati.idData
        WHERE data.anno = (SELECT annoMAX from latestYear)
        ),
    latestDataWithId AS (
    SELECT
        ImmDati.idData
    FROM ImmDati
    LEFT JOIN dimension_Data data
        ON data.idData = ImmDati.idData
    WHERE data.mese = (SELECT meseMAX from latestData) AND
        data.anno = (SELECT annoMAX from latestData)
)
UPDATE ImmDati
SET latest = CASE
                WHEN idData IN (
                    SELECT idData
                    FROM latestDataWithId
                ) THEN 1
                ELSE 0
            END;
    """
)

conn.close()

conn = duckdb.connect(
    r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db"
)


def clean_and_convert_data(df):
    for col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .apply(lambda x: x.encode("latin-1", "ignore").decode("latin-1"))
        )
    return df


# Funzione per eseguire una query, pulire i dati ed esportare in .parquet
def export_to_csv(query, file_path):
    try:
        df = conn.execute(query).fetchdf()
        df = clean_and_convert_data(df)
        df.to_parquet(file_path, index=False)
        print(f"File {file_path} creato con successo.")
    except Exception as e:
        print(f"Errore durante l'esportazione del file {file_path}: {e}")


# Query per ottenere i dati dalle viste di DUckDB
query_fact = "SELECT * FROM fact_Dati"
query_evento = "SELECT * FROM dimension_Evento"
query_data = "SELECT * FROM dimension_Data"
query_tipo_veicolo = "SELECT * FROM dimension_TipoVeicolo"
query_fact_reg = "SELECT * FROM fact_Immatricolazioni"
query_mercato = "SELECT * FROM dimension_Mercato"
query_alimentazione = "SELECT * FROM dimension_Alimentazione"

# Esportazione delle viste in file CSV
export_to_csv(
    query_fact,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_Dati.parquet",
)
export_to_csv(
    query_evento,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Evento.parquet",
)
export_to_csv(
    query_data,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Data.parquet",
)
export_to_csv(
    query_tipo_veicolo,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_TipoVeicolo.parquet",
)
export_to_csv(
    query_fact_reg,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_ImmDati.parquet",
)
export_to_csv(
    query_mercato,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Mercato.parquet",
)
export_to_csv(
    query_alimentazione,
    r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Alimentazione.parquet",
)
conn.close()


# # # ZONA EMERGENZE: Se il report esce dopo due mesi, cancellare i record più nuovi e far rigirare il sistema che applica isLatest.
# # # ATTENZIONE: far girare SOLO la logica che applica latest. Non il resto, o tutto verrà riaggiornato al dopo agosto.

# conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

# conn.execute("DELETE FROM Dati WHERE idData > 20250731")

# conn.execute("DELETE FROM ImmDati WHERE idData > 20250731")

# conn.execute(
#     """
#     WITH latestData AS (
#         SELECT
#         idDato,
#         idTipoVeicolo,
#         MAX(idData) as idDataMAX
#         FROM Dati
#         GROUP BY idDato, idTipoVeicolo
#         )
#     UPDATE Dati
#     SET latest = CASE
#                     WHEN(idDato, idTipoVeicolo, idData) IN (SELECT idDato, idTipoVeicolo, idDataMAX FROM LatestData) THEN 1
#                     ELSE 0
#                 END;
#     """
#     )

# conn.execute(
#     """
#     WITH latestYear AS (
#         SELECT
#         MAX(anno) as annoMAX
#         FROM ImmDati
#         INNER JOIN dimension_Data data
#             ON data.idData = ImmDati.idData
#         ),
#     latestData AS (
#         SELECT
#         MAX(mese) as meseMAX,
#         MAX(anno) as annoMAX
#         FROM ImmDati
#         INNER JOIN dimension_Data data
#             ON data.idData = ImmDati.idData
#         WHERE data.anno = (SELECT annoMAX from latestYear)
#         ),
#     latestDataWithId AS (
#     SELECT
#         ImmDati.idData
#     FROM ImmDati
#     LEFT JOIN dimension_Data data
#         ON data.idData = ImmDati.idData
#     WHERE data.mese = (SELECT meseMAX from latestData) AND
#         data.anno = (SELECT annoMAX from latestData)
# )
# UPDATE ImmDati
# SET latest = CASE
#                 WHEN idData IN (
#                     SELECT idData
#                     FROM latestDataWithId
#                 ) THEN 1
#                 ELSE 0
#             END;
#     """
#     )

# query_fact = "SELECT * FROM fact_Dati"
# query_evento = "SELECT * FROM dimension_Evento"
# query_data = "SELECT * FROM dimension_Data"
# query_tipo_veicolo = "SELECT * FROM dimension_TipoVeicolo"
# query_fact_reg = "SELECT * FROM fact_Immatricolazioni"
# query_mercato = "SELECT * FROM dimension_Mercato"
# query_alimentazione = "SELECT * FROM dimension_Alimentazione"

# # Esportazione delle viste in file CSV
# export_to_csv(query_fact, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_Dati.parquet")
# export_to_csv(query_evento, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Evento.parquet")
# export_to_csv(query_data, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Data.parquet")
# export_to_csv(query_tipo_veicolo, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_TipoVeicolo.parquet")
# export_to_csv(query_fact_reg, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_ImmDati.parquet")
# export_to_csv(query_mercato, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Mercato.parquet")
# export_to_csv(query_alimentazione, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Alimentazione.parquet")

# conn.close()


# obs_df = prepped_reg_df[prepped_reg_df["nomeMercato"] == "AUTOVETTURE"]
# obs_df['idData'] = pd.to_datetime(obs_df['idData'], format='%Y%m%d')

# # Filter the DataFrame for the year 2025
# obs_df_2025 = obs_df[obs_df['idData'].dt.year == 2025]

# # Create a column for the year-month
# obs_df_2025['YearMonth'] = obs_df_2025['idData'].dt.to_period('M')

# # Group by YearMonth and sum the CONTEGGIO
# monthly_summary = obs_df_2025.groupby('YearMonth')['CONTEGGIO'].sum().reset_index()

# # Format the YearMonth for display
# monthly_summary['YearMonth'] = monthly_summary['YearMonth'].astype(str)

# # Display the resulting monthly summary
# print(monthly_summary)

# conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

# duckdb_df = conn.execute("""SELECT * FROM ImmDati
#              """).fetch_df()

# # conn.execute(""" UPDATE Dati SET valore = -0.102695477151044 WHERE idValore = 134393""")

# conn.close()

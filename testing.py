# -*- coding: utf-8 -*-
"""
Created on Tue Jun  3 09:26:29 2025

@author: DiMartino
"""

from pyistat import get, search
import pandas as pd

event_dict = {
    "PIL":1,
    "CrescitaPIL":2,
    "Disoccupazione":3,
    "Inflazione":4,
    "FiduciaImprese":5,
    "TassiDiInteresse":6,
    "BeniEnergetici":7,
    "CarburanteTrasporti":8,
    "PrezzoOro":154,
    "PrezzoRame":155,
    "PrezzoArgento":156,
    "PrezzoGasolio":157,
    "PrezzoPetrolio":158,
    "PrezzoAlluminio":187,
    "PrezzoGasNaturale":188,
    "PrezzoFerroGrezzo":189,
    "PrezzoLitio":190,
    "PrezzoCobalto":191,
    "PrezzoTerreRare":192,
    "TotaleIndiceWCI":266,
    "RotterdamIndiceWCI":267,
    "LosAngelesIndiceWCI":268,
    "GenovaIndiceWCI":269,
    "NewYorkIndiceWCI":270,
}


def extract_date(df):
    df["idData"] = ""
    for index, row in df.iterrows():
        date = str(row["TIME_PERIOD"])
        if "q" in date.casefold():
            year = date[:4]
            quarter = date[-1]
            month = "01" if quarter == "1" else "04" if quarter == "2" else "07" if quarter == "3" else "10"
            iddata = str(year)+str(month)+"01"
            df.at[index, "idData"] = iddata
        elif len(date) == 4:
            year = str(date)
            iddata = year+"0101"
            df.at[index, "idData"] = iddata
        else:
            year = date[:4]
            month = date[-2:]
            iddata = str(year)+str(month)+"01"
            df.at[index, "idData"] = iddata
    return df

def prepare_df(df):
    prepped_df = pd.DataFrame(columns=["Evento","Valore","idData","NomePipeline"])
    df = extract_date(df)
    prepped_df["idData"] = df["idData"]
    prepped_df["Valore"] = df["OBS_VALUE"]
    return prepped_df

def assign_id(df, event_mapping=event_dict):
    prepped_df = df
    prepped_df["idEvento"] = df["Evento"].map(event_mapping)
    return prepped_df


all_df = search.search_dataflows("Prodotto interno lordo", lang="it")

dimensions_df = get.get_dimensions("168_760_DF_DCSP_IPCA1B2015_2")

inflation_df = get.get_data("167_742", dimensions=["A", "IT","", "4","00"])

pil_df = get.get_data("163_156_DF_DCCN_SQCQ_3", dimensions=["Q","IT","","L_2020","N",""], force_url=True)

pil_growth_df = get.get_data("163_156_DF_DCCN_SQCQ_2", dimensions=["Q","IT","B1GQ_B_W2_S1","GO1", "Y","2025M5"])

fuel_energy_df = get.get_data("168_760_DF_DCSP_IPCA1B2015_2", dimensions=["","","4","","ENRGY_5DG"], force_url=True)
energy_df = get.get_data("168_760_DF_DCSP_IPCA1B2015_2", dimensions=["","","","4","FUELS_5DG"], force_url=True)

unemployment_df = get.get_data("151_874_DF_DCCV_TAXDISOCCUMENS1_1", dimensions=["","","","N","9","Y15-74","2025M6G3"])

trust_df = get.get_data("6_64_DF_DCSC_IESI_2", dimensions=["","","","",""])

prepped_pil_df = prepare_df(pil_df)
prepped_pil_df["Evento"] = "PIL"
prepped_pil_df["NomePipeline"] = "Istat"
prepped_pil_df = assign_id(prepped_pil_df)
prepped_inflation_df = prepare_df(inflation_df)
prepped_inflation_df["Evento"] = "Inflazione"
prepped_inflation_df["NomePipeline"] = "Istat"
prepped_inflation_df = assign_id(prepped_inflation_df)
prepped_energy_df = prepare_df(energy_df)
prepped_energy_df["Evento"] = "BeniEnergetici"
prepped_energy_df["NomePipeline"] = "Istat"
prepped_energy_df = assign_id(prepped_energy_df)
prepped_fuel_energy_df = prepare_df(energy_df)
prepped_fuel_energy_df["Evento"] = "CarburanteTrasporti"
prepped_fuel_energy_df["NomePipeline"] = "Istat"
prepped_fuel_energy_df = assign_id(prepped_fuel_energy_df)
prepped_trust_df = prepare_df(trust_df)
prepped_trust_df["Evento"] = "FiduciaImprese"
prepped_trust_df["NomePipeline"] = "Istat"
prepped_trust_df = assign_id(prepped_trust_df)
prepped_pil_growth_df = prepare_df(pil_growth_df)
prepped_pil_growth_df["Evento"] = "CrescitaPIL"
prepped_pil_growth_df["NomePipeline"] = "Istat"
prepped_pil_growth_df = assign_id(prepped_pil_growth_df)
prepped_unemployment_df = prepare_df(unemployment_df)
prepped_unemployment_df["Evento"] = "Disoccupazione"
prepped_unemployment_df["NomePipeline"] = "Istat"
prepped_unemployment_df = assign_id(prepped_unemployment_df)
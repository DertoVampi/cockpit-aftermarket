# -*- coding: utf-8 -*-
"""
Created on Mon Mar  3 16:09:20 2025

@author: DiMartino
"""

# from ecbdata import ecbdata
import datetime as dt

# df = ecbdata.get_series('FM.B.U2.EUR.4F.KR.MRR_FR.LEV')

import imfp
databases = imfp.imf_databases()
focus_commodities = ["pcopp", "pgaso", "pgold", "psilver", "poilapsp", "pallmeta", "piorecr", "plith", "pmeta", "palum", "pcoba", "preodom"]
focus_commodities = [x.upper() for x in focus_commodities]
id_database = "PCPS"
params = imfp.imf_parameters(id_database)
print(params)
current_month = dt.date.today().month
focus_year = dt.date.today().year if current_month != 1 else dt.date.today().year-1
commodity_df = imfp.imf_dataset(
    database_id=id_database,
    freq=["M"],
    unit_measure = "USD",
    start_year=2000,
    end_year=focus_year,)

commodity_df = commodity_df[commodity_df["commodity"].isin(focus_commodities)]
unique_commodities = list(commodity_df["commodity"].unique())
focus_unique_commodities = [commodity for commodity in unique_commodities if commodity in focus_commodities]

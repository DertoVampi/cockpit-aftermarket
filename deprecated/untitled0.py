# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 12:43:16 2025

@author: DiMartino
"""
import pandas as pd
import requests
from istatapi import discovery, retrieval

# def get_all_metadata():
#     final_df = pd.DataFrame(columns=["df_id","version","df_description","df_structure_id","dimension","dimension_ID","description", "values_ids", "values_desc"])
#     all_data = []
#     dimension_values = []
#     df = discovery.all_available()
#     df.to_csv("discovery.xlsx")
#     for index, row in df.iterrows():
#         ds = discovery.DataSet(dataflow_identifier=f"{row['df_id']}")
#         dimension_df = ds.dimensions_info()
#         for index_dim, row_dim in dimension_df.iterrows():
#             values_df = ds.get_dimension_values(f"{row_dim['dimension']}")
#             for index_val, row_val in values_df.iterrows():
#                 values = ', '.join(str(value) for value in row_val["values_ids"])
#                 values_desc = ', '.join(str(value) for value in row_val["values_description"])
#                 all_data.append([row["df_id"], row["version"], row["df_description"], row["df_structure_id"], row_dim["dimension"], row_dim["dimension_ID"], row_dim["description"], values, values_desc])
    
#     final_df = pd.DataFrame(all_data, columns=["df_id", "version", "df_description", "df_structure_id", "dimension", "dimension_ID", "description", "values_ids", "values_desc"])
#     return final_df   
        
def get_all_metadata():
    final_df = pd.DataFrame(columns=["df_id", "version", "df_description", "df_structure_id", "dimension", "dimension_ID", "description", "value_id", "value_desc"])
    all_data = []
    dimension_values = []

    df = discovery.all_available()
    df.to_csv("discovery.csv")
    df.to_excel("discovery.xlsx")

    for index, row in df.iterrows():
        try:
            ds = discovery.DataSet(dataflow_identifier=f"{row['df_id']}")
            print(f"{row['df_id']}")
            dimension_df = ds.dimensions_info()

            for index_dim, row_dim in dimension_df.iterrows():
                dimension = row_dim['dimension']
                if dimension in ds.available_values:
                    values_df = ds.get_dimension_values(dimension)

                    for index_val, row_val in values_df.iterrows():
                        # Verifica che 'values_ids' sia una lista o un array
                        if isinstance(row_val["values_ids"], (list, tuple)):
                            values = ', '.join(str(value) for value in row_val["values_ids"])
                            values_desc = ', '.join(str(value) for value in row_val["values_description"])
                        else:
                            values = str(row_val["values_ids"])

                        all_data.append([
                            row["df_id"], row["version"], row["df_description"], row["df_structure_id"],
                            row_dim["dimension"], row_dim["dimension_ID"], row_dim["description"],
                            values
                        ])
                else:
                    print(f"Dimensione {dimension} non disponibile per il dataset {row['df_id']}")

        except Exception as e:
            print(f"Errore con l'identificatore {row['df_id']}: {e}")

    final_df = pd.DataFrame(all_data, columns=["df_id", "version", "df_description", "df_structure_id", "dimension", "dimension_ID", "description", "value_id", "value_desc"])
    return final_df       
        
df = get_all_metadata()
print(df)
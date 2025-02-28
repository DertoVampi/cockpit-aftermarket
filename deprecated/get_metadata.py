# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 10:08:53 2025

@author: DiMartino
"""
import warnings
import urllib3
warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)
import requests
import pandas as pd
import os
import xml.etree.ElementTree as ET


#%%
def obtain_df(file_path):
    df = pd.read_excel(file_path)
    return df


def remove_all_namespaces(xml_root):
    """
    Rimuove i namespace da tutti i tag dell'albero ElementTree.
    Trasforma, ad esempio, {http://...}Dataflow in Dataflow.
    """
    for elem in xml_root.iter():
        if '}' in elem.tag:
            # Se il tag è del tipo "{http://namespace}Dataflow",
            # splittiamo e teniamo solo la parte dopo "}".
            elem.tag = elem.tag.split('}', 1)[1]
    return xml_root

def get_dataflows(istat_url):
    get_url = "https://sdmx.istat.it/SDMXWS/rest/dataflow/IT1"
    response = requests.get(get_url, verify=False)
    data = []
    
    if response.status_code == 200:
        with open("temp_xml.xml", "wb") as file:
            file.write(response.content)
    else:
        print("La richiesta non è andata a buon fine.")
        return None

    # Leggi il contenuto XML dal file
    with open("temp_xml.xml", "r", encoding="utf-8") as file:
        xml_data = file.read()

    # Parsa il contenuto
    root = ET.fromstring(xml_data)
    
    # Rimuove i namespace da tutti i tag
    root = remove_all_namespaces(root)

    # Ora possiamo cercare i nodi senza dover usare i namespace
    for dataflow in root.findall('.//Dataflow'):
        dataflow_id = dataflow.get('id', 'N/A')

        # Trova <Structure> e poi <Ref>
        structure_element = dataflow.find('./Structure')
        if structure_element is not None:
            ref_element = structure_element.find('./Ref')
            if ref_element is not None:
                ref_id = ref_element.get('id', 'N/A')
            else:
                ref_id = 'N/A'
        else:
            ref_id = 'N/A'

        # Trova i Name in italiano e in inglese
        XML_LANG = '{http://www.w3.org/XML/1998/namespace}lang'
        name_it_element = dataflow.find(f'./Name[@{XML_LANG}="it"]')
        name_en_element = dataflow.find(f'./Name[@{XML_LANG}="en"]')


        name_it = name_it_element.text.strip() if name_it_element is not None else 'N/A'
        name_en = name_en_element.text.strip() if name_en_element is not None else 'N/A'

        data.append([dataflow_id, ref_id, name_it, name_en])

    # Rimuovi il file temporaneo
    os.remove("temp_xml.xml")

    # Crea il DataFrame
    df = pd.DataFrame(data, columns=["IDDataFlow", "RefID", "Name_IT", "Name_EN"])
    df.to_excel("output.xlsx", index=False)

    return df
    
def get_metadata(base_url):
    base_df = obtain_df(r"C:\Users\dimartino\Documents\Codice\Python\codici_ricorrenti\cockpit-aftermarket\metadata\all_data.xlsx")
    df = pd.DataFrame(columns=["IDDataFlow", "Name_IT", "Name_EN", "RefID", "Dimension", "Values"])
    data = []
    failed_data = []
    for index, row in base_df.iterrows():
        get_url = base_url.rstrip('"') + f'{row["IDDataFlow"]}'
        print(get_url)
        try:
            response = requests.get(get_url, verify=False, timeout=30)
            if response.status_code == 200:
                with open("temp_xml.xml", "wb") as file:
                    file.write(response.content)
                    file.close()
            else:
                print("La richiesta non è andata a buon fine.")
                failed_data.append(get_url)
                continue
        except Exception as e:
            print(e)
            failed_data.append(get_url)
            continue
        with open("temp_xml.xml", "r") as file:
            xml_data = file.read()
            root = ET.fromstring(xml_data)
            namespaces = {
                'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
                'structure': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure',
                'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common'
            }
            
            # Estrai le informazioni necessarie

            
            # Trova tutti i KeyValue all'interno di CubeRegion
            for key_value in root.findall('.//structure:CubeRegion/common:KeyValue', namespaces):
                dimension = key_value.attrib['id']
                values = [value.text for value in key_value.findall('common:Value', namespaces)]
                values_str = ', '.join(values)
                data.append([row["IDDataFlow"], row["Name_IT"], row["Name_EN"], row["RefID"], dimension, values_str])

                file.close()
            print(len(data))     
        os.remove("temp_xml.xml")
        # Crea il DataFrame
        
    df = pd.DataFrame(data)
    df.to_excel("output.xlsx")
    if failed_data:
    # Crea un DataFrame con una sola colonna 'FailedURL'
        failed_df = pd.DataFrame({'FailedURL': failed_data})
        failed_df.to_csv("failed.txt", index=False, header=True)
    return df, failed_df


base_url = "http://sdmx.istat.it/SDMXWS/rest/v2/availableconstraint/"
df, failed_df = get_metadata(base_url)
# url = "https://sdmx.istat.it/SDMXWS/rest/v2/dataflow/IT1"
# df = get_dataflows(url)

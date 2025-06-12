# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 14:49:12 2025

@author: DiMartino
"""


import sys
import subprocess

# Install packages if missing, useful with pyinstaller and making .exes
required_packages = ["azure.identity", "pyistat", "loginserviceanfia","imfp", "pyarrow", "fastparquet", "azure.identity", "duckdb", "openpyxl", "webdriver_manager", "ecbdata", "selenium", "pyautogui", "shutil", "urllib", "screeninfo", "pandas","threading","sqlalchemy", "pyodbc", "pandas", "ftplib", "pywinauto", "xlsxwriter", "welcome_derto"]

def install_missing_packages(packages):
    for package in packages:
        try:
            __import__(package)
            print(f"Pacchetto '{package}' già installato.")
        except ImportError:
            print(f"Pacchetto '{package}' non trovato. Installazione in corso...")
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", package])
                print(f"'{package}' installato.")
            except subprocess.CalledProcessError as e:
                print(f"Errore durante l'installazione di '{package}': {e}")
            except Exception as e:
                print(f"Errore inaspettato durante l'installazione di '{package}': {e}")

install_missing_packages(required_packages)
import screeninfo
import loginserviceanfia as als
import duckdb
import pandas as pd
import numpy as np
import welcome_derto
import time
from pywinauto.keyboard import send_keys
from sqlalchemy import create_engine, text
from pywinauto import Desktop
import pyautogui
from threading import Timer, Thread
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.keys import Keys
import datetime as dt
import requests
import os
from ecbdata import ecbdata
import duckdb
import urllib
import imfp
import shutil
from pyistat import get, search

# The query extracts from ANFIA's SQL Server database
query = '''
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
	ON omm.codCombustibile = alimentazione.codAlimentazione
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
'''

def move_window_to_primary_monitor(window):
    rect = window.rectangle()
    window.move_window(x=0, y=0)


def get_login_info_from_config():
    config_file = "config.txt"
    if not os.path.exists(config_file):
        # Chiede all'utente di inserire la password e crea il file config.txt
        user = input("Inserisci la tua mail di Microsoft: ")
        password = input("Inserisci la tua password: ")
        with open(config_file, 'w') as file:
            file.write(user + "\n")
            file.write(password)
        print(f"User e password salvati nel file {config_file}.")
    else:
        # Legge la password dal file config.txt
        with open(config_file, 'r') as file:
            user = file.readline().strip()
            password = file.readline().strip()
    return user, password


def simulate_user_login(user, password):
    try:

        time.sleep(3.5)
        app = Desktop(backend='win32').window(title_re=".*autenticaz.*", visible_only=False)
        dlg = app  # Attende che la finestra sia visibile
        dlg.set_focus()
        move_window_to_primary_monitor(dlg)
        time.sleep(1.5)

        send_keys(user)
        send_keys('{ENTER}{TAB}{ENTER}', with_spaces=True)

        time.sleep(2)
        if dlg.wait('ready', timeout=10):
            # Inserisci la password e premi INVIO
            send_keys(password)  # Usa la password letta dal file di configurazione
            send_keys('{ENTER}')  # Conferma con INVIO
            print("Login simulato con successo.")
        else:
            print("Login simulato con successo. La password non è stata necessaria.")
            pass

    except Exception as e:
        print(f"Errore durante la simulazione del login: {e}. Il login è stato effettuato? Allora non preoccuparti, questo messaggio è normale.")

SERVER = 'sql-anfia-bi-prod.database.windows.net'
DATABASE = 'DealerAnalysis'

# Creazione della stringa di connessione per SQLAlchemy
user, password, server, database, *rest = als.get_login_info_from_config()
connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    + urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Authentication=ActiveDirectoryInteractive"
    )
)

def check_screen():
    screen_width, screen_height = pyautogui.size()
    if screen_width != 1920 or screen_height != 1080:
        print(
            f"Attenzione: lo schermo non è adatto per questo script. Trova uno schermo che sia 1920x1080. Proporzioni del tuo schermo: {screen_width}x{screen_height}.")
        stop = True
        return stop, screen_width, screen_height

def adjust_pixels():
    monitor = screeninfo.get_monitors()[0]
    screen_width, screen_height = monitor.width, monitor.height
    x_ratio = 1920 // screen_width
    y_ratio = 1080 // screen_height
    return x_ratio, y_ratio
    
def setup_driver():
    try:
        driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        return driver
    except Exception as e:
        print(f"Riscontrato problema con il driver: {e}.")
        stop = True
        return stop


def zoom_web_page():
    x_ratio, y_ratio = adjust_pixels()
    pyautogui.click(x=800*x_ratio, y=800*y_ratio, button="left")
    pyautogui.keyDown("ctrl")
    for _ in range(2):
        pyautogui.scroll(100*y_ratio)
        time.sleep(0.1)
    pyautogui.keyUp("ctrl")


def open_link(driver, istat_link):
    time.sleep(1)
    try:
        driver.get(istat_link)
        driver.maximize_window()
        time.sleep(8)
    except Exception as e:
        print(f"Errore {e}: impossibile accedere al link {istat_link}.")


def click_xpath_button(driver, xpath, button_name=None):
    time.sleep(1)
    try:
        button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, f"{xpath}"))
        )
        button.click()
    except:
        print(f"Bottone {button_name} non trovato.") if button_name != None\
            else print(f"Bottone {xpath} non trovato.")
        stop = True
        return stop


def click_text_button(driver, text, button_name=None):
    time.sleep(1)
    text.strip('"')
    text.strip("'")
    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, f"//div[contains(text(), '{text}')]"))
        )
        button.click()
    except:
        print(f"Bottone {button_name} non trovato.") if button_name != None\
            else print(f"Bottone {text} non trovato.")
        stop = True
        return stop


def click_css_button(driver, css_selector, button_name=None):
    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, f"{css_selector}"))
        )
        button.click()
    except:
        print(f"Bottone {button_name} non trovato.") if button_name != None\
            else print(f"Bottone {css_selector} non trovato.")
        stop = True
        return stop


def scroll_until_found(driver, element, is_xpath=True):
    x_ratio, y_ratio = adjust_pixels()
    counter = 0
    found = False
    while found == False and counter < 300:
        pyautogui.moveTo(1920*x_ratio//2, 1080*y_ratio//2 + 100*y_ratio)
        if counter == 0:
            pyautogui.scroll(0)
            counter += 1
        else:
            pyautogui.scroll(-150*y_ratio)
            counter += 1
        if is_xpath == True:
            try:
                xpath = driver.find_element(By.XPATH, f"{element}")
                found = True
                print(f"Elemento {element} individuato.")
                return xpath
            except Exception:
                print(f"Elemento non trovato. Continuo a cercare. Tentativo n. {counter+1}...")
        else:
            try:
                css_selector = driver.find_element(By.CSS_SELECTOR, f"{element}")
                found = True
                print(f"Elemento {element} individuato.")
                return css_selector
            except Exception:
                print(f"Elemento non trovato. Continuo a cercare. Tentativo n. {counter+1}...")
    else:
        print(f"Elemento {element} non trovato. Contatta Cosimo.")
    return None


def is_element_visible(driver, by, value):
    try:
        element = driver.find_element(by, value)
        return element.is_displayed()
    except Exception as e:
        print(e)
        return False


def goto_download_excel():
    x_ratio, y_ratio = adjust_pixels()
    dropdown_visible = False
    counter = 0
    while not dropdown_visible and counter < 3:
        x, y = 1760*x_ratio, 242*y_ratio
        pyautogui.moveTo(x, y)
        pyautogui.click(button="left")
        time.sleep(0.5)
        dropdown_visible = is_element_visible(driver, By.XPATH, value="//*[contains(@aria-label, 'Excel')]")
        counter += 1  
        if dropdown_visible == True:
            pyautogui.moveTo(x, y+(40*y_ratio))
            pyautogui.click(button="left")
            time.sleep(0.5)
            return None
        with pyautogui.hold("ctrl"):
            pyautogui.scroll(100*y_ratio)


def remove_download_popup():
    x_ratio, y_ratio = adjust_pixels()
    pyautogui.moveTo(960*x_ratio, 540*y_ratio)
    pyautogui.click(button="left")

def get_df(skiprows=7):
    download_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    last_file = max(
        (os.path.join(download_folder, f) for f in os.listdir(download_folder)
         if os.path.isfile(os.path.join(download_folder, f))),
        key=lambda f: os.path.getmtime(f),
        default=None
    )
    if last_file:
        df = pd.read_excel(last_file, skiprows=skiprows)
        return df
    else:
        return None

current_month = dt.date.today().month
focus_year = dt.date.today().year if current_month != 1 else dt.date.today().year-1
focus_month = dt.date.today().month-1 if current_month != 1 else 12

stop = False
while stop == False:
    check_screen()
    time.sleep(0.1)
    with setup_driver() as driver:
        # PIL
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,DATAWAREHOUSE,1.0/UP_ACC_TRIMES/IT1,163_184_DF_DCCN_PILQ_1,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Correzione: Dati destagionalizzati']")
        click_text_button(driver, "[W] Dati corretti per gli effetti di calendario")
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        pil_df = get_df()
        remove_download_popup()
        # Crescita PIL
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,DATAWAREHOUSE,1.0/UP_ACC_TRIMES/UP_DCCN_SQCQ/IT1,163_156_DF_DCCN_SQCQ_2,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Valutazione: Contributi alla crescita tendenziale del PIL']")
        click_text_button(driver, '[GO1] Contributi alla crescita congiunturale del PIL')
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        pil_growth_df = get_df()
        remove_download_popup()
        # Disoccupazione
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,Z0500LAB,1.0/LAB_OFFER/LAB_OFF_UNEMPLOY/DCCV_TAXDISOCCUMENS1/IT1,151_874_DF_DCCV_TAXDISOCCUMENS1_1,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Correzione: Dati grezzi']")
        click_text_button(driver, "[Y] Dati destagionalizzati")
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        unemployment_df = get_df()
        remove_download_popup()
        # Inflazione
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,Z0400PRI,1.0/PRI_CONWHONAT/DCSP_NIC2B2015/IT1,167_742_DF_DCSP_NIC2B2015_1,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Misura: [4] Numeri indici']")
        click_text_button(driver, "[8] Variazioni percentuali medie annue")
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        inflation_df = get_df(skiprows=6)
        remove_download_popup()
        # Clima di fiducia delle imprese
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,Z0900ENT,1.0/DCSC_IESI/IT1,6_64_DF_DCSC_IESI_2,1.0"
        open_link(driver, link)
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        trust_df = get_df()
        remove_download_popup()
        # Tassi d'interesse BCE
        interest_rates_df = ecbdata.get_series('FM.B.U2.EUR.4F.KR.MRR_FR.LEV')[['TITLE', 'TIME_PERIOD', 'OBS_VALUE']]
        # Prezzi materie prime IMF
        focus_commodities = ["pcopp", "pgaso", "pgold", "psilver", "poilapsp", "piorecr", "plith", "palum", "pcoba", "preodom"]
        focus_commodities = [x.upper() for x in focus_commodities]
        commodity_df = imfp.imf_dataset(
            database_id="PCPS",
            freq=["M"],
            unit_measure = "USD",
            commodity = focus_commodities,
            start_year=2000,
            end_year=focus_year)
        # commodity_df = commodity_df[commodity_df["commodity"].isin(focus_commodities)]
        # Costi energia
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,Z0400PRI,1.0/PRI_HARCONEU/DCSP_IPCA1B2015/IT1,168_760_DF_DCSP_IPCA1B2015_2,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Misura: [4] Numeri indici']")
        click_text_button(driver, "[7] Variazioni percentuali tendenziali")
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        raw_energy_df = get_df(skiprows=6)
        raw_energy_df.columns = raw_energy_df.columns.str.strip()
        raw_energy_df['Tempo'] = raw_energy_df['Tempo'].str.strip()
        remove_download_popup()
        mask = raw_energy_df.iloc[:, 0].isin(["[ENRGY_5DG] Beni energetici",
                                              "[FUELS_5DG] Carburanti e lubrificanti per mezzi di trasporto (dettaglio 5-digit)"])
        energy_df = raw_energy_df[mask]
        # Dati immatricolazioni automotive

        # Dati parco circolante

        stop = True
# %%

event_dict = {
    "PIL":[1,0],
    "CrescitaPIL":[1,1],
    "Disoccupazione":[1,2],
    "Inflazione":[1,3],
    "FiduciaImprese":[1,4],
    "TassiDiInteresse":[2,5],
    "BeniEnergetici":[1,6],
    "CarburanteTrasporti":[1,7]
}

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

ifm_mapping = {
    "PCOPP":"PrezzoRame",
    "PGASO":"PrezzoGasolio",
    "PGOLD":"PrezzoOro",
    "PSILVER":"PrezzoArgento",
    "POILAPSP":"PrezzoPetrolio",
    "PALUM":"PrezzoAlluminio",
    "PNGAS":"PrezzoGasNaturale",
    "PREODOM":"PrezzoTerreRare",
    "PCOBA":"PrezzoCobalto",
    "PLITH":"PrezzoLitio",
    "PIORECR":"PrezzoFerroGrezzo"
    }

quarter_mapping = {
        "Q1":"03",
        "Q2":"06",
        "Q3":"09",
        "Q4":"12"
    }

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
    "ND": 99
}

type_mapping = {
    "AUTOBUS": "01",
    "AUTOCARRI": "02",
    "RIMORCHI LEGGERI": "03",
    "RIMORCHI PESANTI": "04",
    "SEMIRIMORCHI": "05",
    "VEICOLI COMMERCIALI LEGGERI": "06",
    "AUTOVETTURE": "07",
    "SCONOSCIUTO": "99"
}

month_mapping = {
    "01": "Q1", "02": "Q1", "03": "Q1",
    "04": "Q2", "05": "Q2", "06": "Q2",
    "07": "Q3", "08": "Q3", "09": "Q3",
    "10": "Q4", "11": "Q4", "12": "Q4"
}

iam_price_mapping = {
    "Delta % Prezzo Medio Puntuale":"9",
    "Delta % Prezzo Medio YTD":"10",
    "Delta % Prezzo Medio Rolling":"11",
    }

iam_rolling_mapping = {
    "Delta Fatturato":12,
    "Effetto Volumi":13,
    "Effetto Mix CP":14,
    "Effetto Prezzi":15
    }

iam_puntuale_mapping = {
    "Delta Fatturato":16,
    "Effetto Volumi":17,
    "Effetto Mix CP":18,
    "Effetto Prezzi":19,
    }

iam_progressivo_mapping = {   
    "Delta Fatturato":20,
    "Effetto Volumi":21,
    "Effetto Mix CP":22,
    "Effetto Prezzi":23
    }

month_name_mapping = {
    "gennaio":"01",
    "febbraio":"02",
    "marzo":"03",
    "aprile":"04",
    "maggio":"05",
    "giugno":"06",
    "luglio":"07",
    "agosto":"08",
    "settembre":"09",
    "ottobre":"10",
    "novembre":"11",
    "dicembre":"12"
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
    12: "dicembre"
}

def istat_manipulate_quarter_df(df, event=event_dict, mapping=quarter_mapping):
    if df is pil_df:
        df = df.drop([0, 1, 5])
        df.columns = df.columns.str.strip()
        df_melted = df.melt(id_vars=['Tempo'], var_name='Data', value_name='Valore')    
        df_melted = df_melted[df_melted['Valore'] != '..']    
        df_melted['Valore'] = df_melted['Valore'].astype(float)
        df_melted = df_melted.rename(columns={'Tempo': 'Evento'})
        df_melted[['Anno', 'Quarter']] = df_melted['Data'].str.extract(r'(\d{4})-(Q\d)')
        df_melted['Mese'] = df_melted['Quarter'].map(quarter_mapping)
        df_melted['Giorno'] = "01"
        df_melted['idData'] = df_melted['Anno'].astype(str)+df_melted['Quarter'].map(quarter_mapping).astype(str)+"01"
        df_melted["Evento"] = df_melted['Evento'].str.strip()
        df_melted = df_melted[df_melted["Evento"] == "Prodotto interno lordo ai prezzi di mercato"]
        df_melted["Evento"] = 'PIL'
        df_melted['nomePipeline'] = 'Istat'
    else:
        df = df.drop([0, 1, 5])
        df.columns = df.columns.str.strip()
        df_melted = df.melt(id_vars=['Tempo'], var_name='Data', value_name='Valore')    
        df_melted = df_melted[df_melted['Valore'] != '..']    
        df_melted['Valore'] = df_melted['Valore'].astype(float)
        df_melted = df_melted.rename(columns={'Tempo': 'Evento'})
        df_melted[['Anno', 'Quarter']] = df_melted['Data'].str.extract(r'(\d{4})-(Q\d)')
        df_melted['Mese'] = df_melted['Quarter'].map(quarter_mapping)
        df_melted['Giorno'] = "01"
        df_melted['idData'] = df_melted['Anno'].astype(str)+df_melted['Quarter'].map(quarter_mapping).astype(str)+"01"
        df_melted["Evento"] = df_melted['Evento'].str.strip()        
        df_melted["Evento"] = df_melted['Evento'].str.strip()
        df_melted = df_melted[df_melted["Evento"] == "Spesa per consumi finali nazionali"]
        df_melted["Evento"] = 'CrescitaPIL'  
        df_melted['nomePipeline'] = 'Istat'
    return df_melted

def istat_manipulate_monthly_df(df, event=event_dict, mapping=month_mapping):
    df.columns = df.columns.str.strip()
    df['Tempo'] = df['Tempo'].str.strip()
    if df is unemployment_df:
        df.iloc[:, 1] = df.iloc[:, 1].str.strip()
        df = df[df["Tempo"] == "Totale"]
        df = df[df.iloc[:,1] == "15-74 anni"]
        df = df.drop(df.columns[1], axis=1)
        df_melted = df.melt(id_vars=["Tempo"], var_name="Data", value_name="Valore")
        df_melted[['Anno', 'Mese']] = df_melted['Data'].str.split("-", expand=True)
        df_melted['Giorno'] = "01"
        df_melted['Quarter'] = df_melted['Mese'].map(month_mapping).astype(str)
        df_melted['idData'] = df_melted['Anno'].astype(str)+df_melted['Mese'].astype(str)+"01"
        df_melted = df_melted.rename(columns={'Tempo': 'Evento'})   
        df_melted["Evento"] = df_melted["Evento"].replace("Totale", "Disoccupazione")
        df_melted['Valore'] = df_melted['Valore'].astype(float)
        df_melted['nomePipeline'] = 'Istat'
    elif df is trust_df:
        df = df.rename(columns={'Tempo':'Data'})
        df[['Anno', 'Mese']] = df['Data'].str.split("-", expand=True)
        df['Giorno'] = "01"
        df['Quarter'] = df['Mese'].map(month_mapping).astype(str)
        df['idData'] = df['Anno'].astype(str)+df['Mese'].astype(str)+'01'
        df.columns.values[1] = 'Valore'
        df['Valore'] = df['Valore'].astype(float)
        df['Evento'] = 'FiduciaImprese'
        df_melted = df
        df_melted['nomePipeline'] = 'Istat'
    elif df is energy_df:
        df = df.drop(df.columns[-1], axis=1)
        df_melted = df.melt(id_vars=["Tempo"], var_name="Data", value_name="Valore")
        df_melted[['Anno', 'Mese']] = df_melted['Data'].str.split("-", expand=True)
        df_melted['Giorno'] = "01"
        df_melted['Quarter'] = df_melted['Mese'].map(month_mapping).astype(str)
        df_melted['idData'] = df_melted['Anno'].astype(str)+df_melted['Mese'].astype(str)+"01"
        df_melted = df_melted.rename(columns={'Tempo': 'Evento'})   
        df_melted["Evento"] = df_melted["Evento"].replace("[ENRGY_5DG] Beni energetici", "BeniEnergetici").replace("[FUELS_5DG] Carburanti e lubrificanti per mezzi di trasporto (dettaglio 5-digit)","CarburanteTrasporti")
        df_melted['Valore'] = df_melted['Valore'].astype(float)
        df_melted['nomePipeline'] = 'Istat'
    return df_melted

def istat_manipulate_yearly_df(df, event=event_dict):
    df.columns = df.columns.str.strip()
    df['Tempo'] = df['Tempo'].str.strip()
    df = df[df["Tempo"] == "[00] Indice generale"]
    df_melted = df.melt(id_vars=["Tempo"], var_name="Data", value_name="Valore")
    df_melted['Anno'] = df_melted['Data']
    df_melted['idData'] = df_melted['Anno'].astype(str)+"01"+"01"
    df_melted['Giorno'] = "01"
    df_melted['Mese'] = "01"
    df_melted['Quarter'] = "0"
    df_melted = df_melted.rename(columns={'Tempo': 'Evento'})
    df_melted["Evento"] = df_melted["Evento"].replace("[00] Indice generale", "Inflazione")
    df_melted['Valore'] = df_melted['Valore'].astype(float)
    df_melted['nomePipeline'] = 'ISTAT'   
    return df_melted

def eu_manipulate_df(df, event=event_dict, mapping=month_mapping):
    df.columns = df.columns.str.strip()
    df['TITLE'] = df['TITLE'].str.strip()
    df['TITLE'] = df['TITLE'].replace('Main refinancing operations - fixed rate tenders (fixed rate) (date of changes) - Level','TassiDiInteresse')
    df = df.rename(columns={'TITLE':'Evento', 'TIME_PERIOD':'Data', 'OBS_VALUE':'Valore'})
    df[['Anno', 'Mese', 'Giorno']] = df['Data'].str.split("-", expand=True)
    df['idData'] = df['Anno'].astype(str)+df['Mese'].astype(str)+df['Giorno'].astype(str)
    df['Quarter'] = df['Mese'].map(mapping).astype(str)
    df['Valore'] = df['Valore'].astype(float)
    df['nomePipeline'] = 'BCE'
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
        print("Dati IAM del mese scorso non trovati. Continuo senza aggiornare i dati IAM.")
        df = None
    return df

def manipulate_ifm_df(df):
    df.columns = df.columns.str.strip()
    df = df.rename(columns={'time_period':'Data', 'obs_value':'Valore'})
    df[['Anno', 'Mese']] = df['Data'].str.split("-", expand=True)
    df["idData"] = df['Anno'].astype(str)+df['Mese'].astype(str)+"01"
    df["Valore"] = round(df["Valore"].astype(float), 2)
    df["Evento"] = df["commodity"].map(ifm_mapping).astype(str)
    return df

def extract_wci(excel_file_path, skiprows=1):
    if os.path.exists(excel_file_path):
        df = pd.read_excel(excel_file_path, skiprows=skiprows)
    else:
        print("Attenzione: dati del WCI non trovati. Probabilmente sono stati spostati.")
        df = None
    return df

focus_month_name = month_number_mapping[focus_month].capitalize()
excel_file_path = fr"L:\01.Dati\04.Varie\08.Cockpit\csv_database\IAM\Report IAM Trend Italia Distributori IAM- {focus_month_name} {focus_year}.xlsx"
wci_excel_file_path = r"L:\01.Dati\04.Varie\08.Cockpit\files\Drewry_WCI.xlsx"
m_pil_df = istat_manipulate_quarter_df(pil_df)
m_pil_growth_df = istat_manipulate_quarter_df(pil_growth_df)
m_unemployment_df = istat_manipulate_monthly_df(unemployment_df)
m_inflation_df = istat_manipulate_yearly_df(inflation_df)
m_trust_df = istat_manipulate_monthly_df(trust_df)
m_interest_rates_df = eu_manipulate_df(interest_rates_df)
m_energy_df = istat_manipulate_monthly_df(energy_df)
m_energy_price_df, m_energy_fuel_df = manipulate_energy_df(m_energy_df)
m_iam_price_df = extract_iam(excel_file_path, "Prezzi Medi", skiprows=5)
m_iam_car_df = extract_iam(excel_file_path, "Auto-Rolling MERCATO", skiprows=35)
m_iam_truck_df = extract_iam(excel_file_path, "Truck-Rolling MERCATO", skiprows=35)
m_iam_puntuale_car_df = extract_iam(excel_file_path, "Auto-Puntuale MERCATO", skiprows=34)
m_iam_puntuale_truck_df = extract_iam(excel_file_path, "Truck-Puntuale MERCATO", skiprows=34)
m_iam_progressivo_car_df = extract_iam(excel_file_path, "Auto-Progressivo MERCATO", skiprows=33)
m_iam_progressivo_truck_df = extract_iam(excel_file_path, "Truck-Progressivo MERCATO", skiprows=33)
m_commodity_df = manipulate_ifm_df(commodity_df)
m_wci_df = extract_wci(wci_excel_file_path)

try:
    auth_thread = Thread(target=als.simulate_user_login, args=(user, password))
    auth_thread.start()
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        m_reg_df = pd.DataFrame(result.fetchall(), columns=result.keys())
except Exception as e:
    print(e)
    
def prepare_iam_price_df(df, iam_mapping=iam_price_mapping):
    if df is None:
        return df
    focus_month = dt.date.today().month-2 if dt.date.today().month != 1 else 12
    focus_year = dt.date.today().year if dt.date.today().month != 1 else dt.date.today().year-1
    df = df.drop(["Unnamed: 0", "Unnamed: 3", "Unnamed: 2", f"{focus_month_name} {focus_year}"] , axis=1)
    df = df.iloc[:2]
    df["idData"] = str(focus_year)+str(focus_month).zfill(2)+"01"
    df["idTipoVeicolo"] = range(len(df))
    df = df.melt(id_vars=["idData","idTipoVeicolo"], value_vars=["Delta % Prezzo Medio Puntuale","Delta % Prezzo Medio YTD","Delta % Prezzo Medio Rolling"], var_name="nomeEvento", value_name='Valore')
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idPipeline"] = 0
    return df

def prepare_iam_df(df, vehicle_type, iam_mapping, month_name_mapping=month_name_mapping):
    if df is None:
        return df
    df = df.drop(["Unnamed: 0", "Unnamed: 1", "Unnamed: 15"], axis=1)
    df = df.rename(columns={"Unnamed: 2":"nomeEvento"})

    df["nomeEvento"] = df["nomeEvento"].replace("∆ Fatturato", "Delta Fatturato")
    df_columns = df.columns.tolist()
    ordered_cols = df_columns[1:13]
    df = df.melt(id_vars="nomeEvento", value_vars=ordered_cols, var_name="Mese", value_name="Valore")
    df["numMese"] = df["Mese"].str.casefold().map(month_name_mapping)
    january_index = df[df["numMese"] == "01"].index[0]
    df = df[df["Mese"] != "Unnamed: 3"]
    df["idPipeline"] = 0
    df["Anno"] = [dt.date.today().year-1 if i < january_index else dt.date.today().year for i in range(len(df))]
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idData"] = df["Anno"].astype(str)+df["numMese"].astype(str)+"01"
    if vehicle_type.casefold() == "car":
        df["idTipoVeicolo"] = 0
    elif vehicle_type.casefold() == "truck":
        df["idTipoVeicolo"] = 1
    return df

def prepare_iam_df_prog(df, vehicle_type, iam_mapping, month_name_mapping=month_name_mapping):
    if df is None:
        return df
    df = df.drop(["Unnamed: 0", "Unnamed: 1", "Progressivo"], axis=1)
    df = df.rename(columns={"Unnamed: 2":"nomeEvento"})
    df["nomeEvento"] = df["nomeEvento"].replace("∆ Fatturato", "Delta Fatturato")
    df_columns = df.columns.tolist()
    ordered_cols = df_columns[1:13]
    df = df.melt(id_vars="nomeEvento", value_vars=ordered_cols, var_name="Mese", value_name="Valore")
    df["numMese"] = df["Mese"].str.casefold().map(month_name_mapping)
    df = df[df["Mese"] != "Unnamed: 3"]
    df = df[~pd.isna(df["Valore"])]
    df["idPipeline"] = 0
    df["Anno"] = dt.date.today().year-1 if dt.date.today().month == 1 else dt.date.today().year
    df["idDato"] = df["nomeEvento"].map(iam_mapping)
    df["idData"] = df["Anno"].astype(str)+df["numMese"].astype(str)+"01"
    if vehicle_type.casefold() == "car":
        df["idTipoVeicolo"] = 0
    elif vehicle_type.casefold() == "truck":
        df["idTipoVeicolo"] = 1
    return df

def prepare_registration_df(df, fuel_mapping=fuel_mapping, type_mapping=type_mapping):
    if df is None:
        return df
    df = df.drop(["DATA_IMMATRICOLAZIONE"], axis=1)
    df = df.rename(columns={"CATEGORIA_VEICOLO":"nomeMercato", "ALIMENTAZIONE":"nomeAlimentazione"})
    df["nomeMercato"] = df["nomeMercato"].fillna("SCONOSCIUTO")
    df["idMercato"] = df["nomeMercato"].map(type_mapping) 
    df["idAlimentazione"] = df["nomeAlimentazione"].map(fuel_mapping)
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
    df = df.melt(id_vars=["idData"], value_vars=["TotaleIndiceWCI","RotterdamIndiceWCI","LosAngelesIndiceWCI","GenovaIndiceWCI","NewYorkIndiceWCI"], var_name="nomeEvento", value_name="Valore")
    df["idDato"] = df["nomeEvento"].map(wci_mapping)
    return df

prepped_pil_df = prepare_df(m_pil_df)
prepped_pil_growth_df = prepare_df(m_pil_growth_df)
prepped_unemployment_df = prepare_df(m_unemployment_df)
prepped_inflation_df = prepare_df(m_inflation_df)
prepped_trust_df = prepare_df(m_trust_df)
prepped_interest_rates_df = prepare_df(m_interest_rates_df)
prepped_energy_price_df = prepare_df(m_energy_price_df)
prepped_energy_fuel_df = prepare_df(m_energy_fuel_df)
prepped_iam_price_df = prepare_iam_price_df(m_iam_price_df)
prepped_iam_car_df = prepare_iam_df(m_iam_car_df, vehicle_type="car", iam_mapping=iam_rolling_mapping)
prepped_iam_truck_df = prepare_iam_df(m_iam_truck_df, vehicle_type="truck", iam_mapping=iam_rolling_mapping)
prepped_iam_puntuale_car_df = prepare_iam_df_prog(m_iam_puntuale_car_df, vehicle_type="car", iam_mapping=iam_puntuale_mapping)
prepped_iam_puntuale_truck_df = prepare_iam_df_prog(m_iam_puntuale_truck_df, vehicle_type="truck", iam_mapping=iam_puntuale_mapping)
prepped_iam_progressivo_car_df = prepare_iam_df_prog(m_iam_progressivo_car_df, vehicle_type="car", iam_mapping=iam_progressivo_mapping)
prepped_iam_progressivo_truck_df = prepare_iam_df_prog(m_iam_progressivo_truck_df, vehicle_type="truck", iam_mapping=iam_progressivo_mapping)
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
    prepped_commodity_df
    ]   
   
iam_df_list = [
    prepped_iam_price_df,
    prepped_iam_car_df,
    prepped_iam_truck_df,
    prepped_iam_puntuale_car_df,
    prepped_iam_puntuale_truck_df,
    prepped_iam_progressivo_car_df,
    prepped_iam_progressivo_truck_df
    ]

df_list = [prepped_commodity_df]
 
conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

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
                (row['idDato'], row['idData'], row['Valore'])
            )

if prepped_wci_df is not None:
    for index, row in prepped_wci_df.iterrows():
        conn.execute("""
                     INSERT INTO Dati (idDato, idData, valore, idTipoVeicolo, latest)
                     VALUES (?, ?, ?, 2, 0)
                     ON CONFLICT (idDato, idData, idTipoVeicolo)
                     DO UPDATE SET valore = EXCLUDED.valore
                     """,
                     (row['idDato'], row['idData'], row['Valore'])
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
                (row['idDato'], row['idData'], row['idTipoVeicolo'], row['Valore'])
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
        (row['idData'], row['idAlimentazione'], row['idMercato'], row['CONTEGGIO'])
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

conn = duckdb.connect(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db")

def clean_and_convert_data(df):
    for col in df.columns:
        df[col] = df[col].astype(str).apply(lambda x: x.encode('latin-1', 'ignore').decode('latin-1'))
    return df

# Funzione per eseguire una query, pulire i dati ed esportare in CSV
def export_to_csv(query, file_path):
    try:
        df = conn.execute(query).fetchdf()
        df = clean_and_convert_data(df)
        df.to_parquet(file_path, index=False)
        print(f"File {file_path} creato con successo.")
    except Exception as e:
        print(f"Errore durante l'esportazione del file {file_path}: {e}")

# Query per ottenere i dati dalle viste
query_fact = "SELECT * FROM fact_Dati"
query_evento = "SELECT * FROM dimension_Evento"
query_data = "SELECT * FROM dimension_Data"
query_tipo_veicolo = "SELECT * FROM dimension_TipoVeicolo"
query_fact_reg = "SELECT * FROM fact_Immatricolazioni"
query_mercato = "SELECT * FROM dimension_Mercato"
query_alimentazione = "SELECT * FROM dimension_Alimentazione"

# Esportazione delle viste in file CSV
export_to_csv(query_fact, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_Dati.parquet")
export_to_csv(query_evento, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Evento.parquet")
export_to_csv(query_data, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Data.parquet")
export_to_csv(query_tipo_veicolo, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_TipoVeicolo.parquet")
export_to_csv(query_fact_reg, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\fact_ImmDati.parquet")
export_to_csv(query_mercato, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Mercato.parquet")
export_to_csv(query_alimentazione, r"L:\01.Dati\04.Varie\08.Cockpit\csv_database\dimension_Alimentazione.parquet")
conn.close()
# shutil.copy(r"L:\07.Automobile_in_Cifre\2024\StatisticheItalia\parco\CapA\Aggiornati (e caricati)\05TipoAnnoImmat.xlsx", r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\csv_database\parco.xlsx")
# shutil.copy(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpit.db", r"L:\01.Dati\04.Varie\08.Cockpit\Report_cockpit\cockpit.db")
# shutil.copy(r"C:\Users\dimartino\OneDrive - anfia.it\cockpit_anfia\pbix\cockpitpbi.pbix", r"L:\01.Dati\04.Varie\08.Cockpit\Report_cockpit\cockpitpbi.pbix")
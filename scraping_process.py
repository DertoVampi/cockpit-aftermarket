# -*- coding: utf-8 -*-
"""
Created on Fri Feb 28 14:49:12 2025

@author: DiMartino
"""
import pandas as pd
import welcome_derto
import time
import pyautogui
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from selenium.webdriver.common.keys import Keys
import datetime as dt
import requests
import os


def check_screen():
    screen_width, screen_height = pyautogui.size()
    if screen_width != 1920 or screen_height != 1080:
        print(
            f"Attenzione: lo schermo non è adatto per questo script. Trova uno schermo che sia 1920x1080. Proporzioni del tuo schermo: {screen_width}x{screen_height}.")
        stop = True
        return stop


def setup_driver():
    try:
        driver = webdriver.Edge(service=Service(EdgeChromiumDriverManager().install()))
        return driver
    except Exception as e:
        print(f"Riscontrato problema con il driver: {e}.")
        stop = True
        return stop


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
        button = WebDriverWait(driver, 10).until(
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
    counter = 0
    found = False
    while found == False and counter < 300:
        pyautogui.moveTo(1920//2, 1080//2 + 100)
        if counter == 0:
            pyautogui.scroll(0)
            counter += 1
        else:
            pyautogui.scroll(-150)
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


def goto_download_excel():
    x, y = 1820, 250
    pyautogui.moveTo(x, y)
    pyautogui.click(button="left")
    time.sleep(0.5)
    pyautogui.moveTo(x, y+40)
    pyautogui.click(button="left")
    time.sleep(0.5)


def get_df():
    download_folder = os.path.join(os.path.expanduser("~"), "Downloads")
    last_file = max(
        (os.path.join(download_folder, f) for f in os.listdir(download_folder)
         if os.path.isfile(os.path.join(download_folder, f))),
        key=lambda f: os.path.getmtime(f),
        default=None
    )
    if last_file:
        df = pd.read_excel(last_file, skiprows=7)
        return df
    else:
        return None


stop = False
while stop == False:
    check_screen()
    time.sleep(0.1)
    with setup_driver() as driver:
        link = "https://esploradati.istat.it/databrowser/#/it/dw/categories/IT1,DATAWAREHOUSE,1.0/UP_ACC_TRIMES/IT1,163_184_DF_DCCN_PILQ_1,1.0"
        open_link(driver, link)
        click_xpath_button(driver, "//*[@aria-label='Correzione: Dati destagionalizzati']")
        click_text_button(driver, "[W] Dati corretti per gli effetti di calendario")
        time.sleep(1)
        goto_download_excel()
        click_xpath_button(driver, "//button[contains(text(), 'Scarica')]")
        time.sleep(3)
        pil_df = get_df()
        stop = True

import os
import re
import sys
import json
import psycopg2
import requests
import configparser

from bs4 import BeautifulSoup
from selenium import webdriver
from psycopg2.extras import Json
from requests.exceptions import HTTPError
from requests.exceptions import TooManyRedirects
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC


assert (len(sys.argv) == 3), "Too few/many arguments"
wtype = sys.argv[1]
city = (sys.argv[2]).lower()

wtype_d = {
    "0": "Wohnung",
    "1": "Haus",
    "2": "Grundstück",
    "3": "Zwangsversteigerung",
    "4": "Anlageobjekt"
}

# path of script
script_path = os.path.dirname(os.path.realpath(__file__))

# read config
config = configparser.ConfigParser()
config.read(script_path + "/cfg.ini")

# connect to db
conn = psycopg2.connect(
    dbname=config["DATABASE"]["dbname"],
    user=config["DATABASE"]["user"],
    host=config["DATABASE"]["host"]
)

conn.autocommit = True
cur = conn.cursor()

images_sql = """
    INSERT INTO spk.images_inserate(image, id, tag)
    VALUES (%s, %s, %s);
    """

inserat_sql = """
    INSERT INTO spk.inserate (such_str, fio_id, objektkategorie, geo, preise,
    flaechen, ausstattung, zustand_angaben, freitexte, verwaltung_objekt,
    verwaltung_techn, anbieter, sip, wohnungs_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

inserat_ids_sql = """
    SELECT fio_id
    FROM spk.inserate;
    """

# get stored inserat_ids
cur.execute(inserat_ids_sql)
rows = cur.fetchall()
inserat_ids = [x[0] for x in rows] if len(rows) > 0 else []
print(f"Bisher {len(inserat_ids)} inserate für {city} und {wtype_d[wtype]}")

def http_get(url):
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r
        else:
            raise requests.HTTPError(
                f"Request failed with status_code {r.status_code}"
            )
    except requests.ConnectionError as e:
        print(url + " probably offline!")
        raise e

def http_get_to_soup(url):
    r = http_get(url)
    # only allow content type "text/html"
    if "text/html" in r.headers["Content-Type"]:
        return BeautifulSoup(r.text, "lxml")

    else:
        print(f"Expected Content Type text/html, but got \
              {r.headers['Content-Type']} instead")
        raise TypeError

def parse_inserat(soup):
    pass

# selenium Page interaktion mit Firefox headless
options = Options()
options.headless = True
driver = webdriver.Firefox(options=options)
# driver = webdriver.Firefox()
driver.get("https://immobilien.sparkasse.de")
input_element = "//div/input[@placeholder='PLZ / Ort / SIP-ID*']"
city_input = driver.find_element_by_xpath(input_element)

# Nach Stadt suchen und "Suchen" Button klicken
city_input.send_keys(city)
suchen_btn = WebDriverWait(driver, 10).until(
    EC.presence_of_element_located((By.CLASS_NAME, "btn-label"))
)
suchen_btn.click()

try:
    # solange auf weitere Ergebnisse drücken, bis Button verschwindet
    while True:
        button_str = "//div[@class='sip-estate-list']//span/button/div/span"
        element = WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, button_str))
        )
        driver.find_element_by_xpath(button_str).click()
except (NoSuchElementException, TimeoutException):
    print("keine weiteren Pages mehr")
finally:
    page_all = driver.page_source
    driver.quit()

soup = BeautifulSoup(page_all, "lxml")
links = soup.find_all("a")

# SPI Id  Beispiel: '/FIO-10915855820'
p = re.compile(r"/FIO-\d*")
fio_ids = p.findall(",".join([x.get("href") for x in links]))

# bereits bearbeitete Inserate uebersrpingen
# x[1:] weil "/" nicht nicht ids enthalten
fio_ids = [x for x in fio_ids if x[1:] not in inserat_ids]
print(fio_ids)

for fio in fio_ids:
    inserat_url = f"https://immobilien.sparkasse.de{fio}#?detailPage=1"
    print(f"parsing {inserat_url}")
    soup = http_get_to_soup(inserat_url)
    inserat = soup.find("input", {"name": "estate"})
    inserat_data = json.loads(inserat.get("value"))
    inserat_data = inserat_data["estate"]

    # prepare list for insert in inserate
    preped_l = [
        city,
        inserat_data["id"],
        Json(inserat_data["objektkategorie"]),
        Json(inserat_data["geo"]),
        Json(inserat_data["preise"]),
        Json(inserat_data["flaechen"]),
        Json(inserat_data["ausstattung"]),
        Json(inserat_data["zustand_angaben"]),
        Json(inserat_data["freitexte"]),
        Json(inserat_data["verwaltung_objekt"]),
        Json(inserat_data["verwaltung_techn"]),
        Json(inserat_data["anbieter"]),
        Json(inserat_data["sip"]),
        wtype
    ]

    cur.execute(inserat_sql, preped_l)

    # get images from "anhaenge"
    images_data = [
        x for x in inserat_data["anhaenge"] if x["format"] in ("JPG", "PNG")
    ]

    for i in images_data:
        # multiple formats for images -> only take "original"
        orig = [x for x in i["data"] if "original" in x]
        assert len(orig) == 1
        orig_url = orig[0]["original"]
        r = http_get(orig_url)

        # anhangtitel sometimes missing; use attributes instead
        try:
            tag = i["anhangtitel"]
        except KeyError:
            tag = i["@attributes"]["gruppe"]

        cur.execute(
            images_sql,
            [r.content, inserat_data["id"], tag]
        )

cur.close()
conn.close()

import sys
import psycopg2
import requests
import configparser

from bs4 import BeautifulSoup
from psycopg2.extras import Json
from datetime import datetime, timedelta
from requests.exceptions import TooManyRedirects, HTTPError


assert (len(sys.argv) == 3), "Too few/many arguments"
wtype = sys.argv[1]
city = sys.argv[2].lower()

# main url
url = "https://www.immobilienscout24.de/"

# expose-url
expo_url = url + "expose/"

wtype_d = {
    "0": "wohnung-mieten"
}

def http_get(url):
    try:
        r = requests.get(url)
        # check if captcha
        if "https://www.wg-gesucht.de/cuba.html" in r.url:
            raise TooManyRedirects("Captcha appeared! Exit")

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

def parse_expose(data_id):
    soup = http_get_to_soup(expo_url + data_id)

    # Titel
    titel = soup.find("h1", id="expose-title").text

    # Adresse
    adress_block = soup.find("div", class_="address-block")
    adress_parts = [x.text for x in adress_block.find_all("span")]

    s = 'Die vollständige Adresse der Immobilie erhalten Sie vom Anbieter.'

    # ohne Adress Details
    if s in adress_parts:
        adress_str = adress_parts[0].replace(",", "")
        plz = adress_str.split()[0]
        viertel = adress_str.split()[-1]
        adress_str = " ".join(adress_str.split()[1:])
        adress_str = adress_str.strip()
    else:
        plz = adress_parts[1].split()[0]
        viertel = adress_parts[1].split()[-1]
        # get rid of plz
        adress_main = " ".join(adress_parts[1].strip().split()[1:])
        adress_combed = " ".join(
            [adress_main, adress_parts[0].strip()]
        )
        adress_str = adress_combed.replace(",", "")

    ret_d = {
        "titel": titel,
        "plz": plz,
        "adress_str": adress_str,
        "viertel": viertel
    }


# page counter
count = 1
get_string = f"{url}Suche/de/bayern/{city}/{wtype_d[wtype]}?pagenumber={count}"

soup = http_get_to_soup(get_string)
items_l = soup.find("ul", id="resultListItems")
result_listing = items_l.find_all("li", class_="result-list__listing")

data_ids = [x.get("data-id") for x in result_listing]
print(data_ids)

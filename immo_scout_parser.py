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

# page counter
count = 1
get_string = f"{url}Suche/de/bayern/{city}/{wtype_d[wtype]}?pagenumber={count}"

soup = http_get_to_soup(get_string)
items_l = soup.find("ul", id="resultListItems")
result_listing = items_l.find_all("li", class_="result-list__listing")

data_ids = [x.get("data-id") for x in result_listing]
print(data_ids)

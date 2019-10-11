from bs4 import BeautifulSoup
import requests


def url_parser(stadt, wtype, url="https://www.wg-gesucht.de/"):
    """

    :url: zu parsende URL (String)
    :stadt: welche Stadt? (String)
    :wtyp: welcher wohnungstyp? (String)
    :value_dict = {
        1: "wg",
        2: "1-zimmer-wohnung",
        3: "wohnung",
        4: haus
    }
    :returns: urls der verfuegbaren wohnungen

    """

    wtype_dict = {
        1: "wg-zimmer",
        2: "1-zimmer-wohnungen",
        3: "wohnungen",
        4: "haeuser"
    }

    city_codes = {
        "Augsburg": "2"
    }

    # counter fuer pages
    p_cnt = 0
    get_string = url + wtype_dict[wtype] + "-in-" + stadt + "." \
    + city_codes["Augsburg"] + ".0.0." + f"{p_cnt}" + ".html"
    r = requests.get(get_string)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, "lxml")
    else:
        raise requests.HTTPError(f"Request failed with status_code {r.status_code}")
    # get available pages
    page_bar = soup.find_all("ul", class_="pagination pagination-sm")[0]
    page_counter = page_bar.find_all("li")[-2].get_text().strip()
    print(page_counter)
    # wgs list from "main column"
    wgs_body = soup.find("div", id="main_column").table.tbody.find_all("tr")
    # filter out AirBnb offers
    wgs = [x.find("td", class_="ang_spalte_datum row_click") for x in wgs_body]
    # filter out None Types
    wgs_filter = list(filter(None, wgs))
    urls = ["https://www." + x.a["href"] for x in wgs_filter]

    return urls

print(url_parser("Augsburg", 1))

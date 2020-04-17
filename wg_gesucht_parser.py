import os
import re
import sys
import time
import psycopg2
import requests
import configparser

from bs4 import BeautifulSoup
from psycopg2.extras import Json
from datetime import datetime, timedelta
from requests.exceptions import TooManyRedirects


assert (len(sys.argv) == 3), "Too few/many arguments"
wtype = sys.argv[1]
city = sys.argv[2]

# path of script
script_path = os.path.dirname(os.path.realpath(__file__))
# main url
url = "https://www.wg-gesucht.de/"

wtype_d = {
    "0": "wg-zimmer",
    "1": "1-zimmer-wohnungen",
    "2": "wohnungen",
    "3": "haeuser"
}

city_codes = {
    "Augsburg": "2",
    "Munchen": "90"
}

get_string = f"{url}{wtype_d[wtype]}-in-{city}.{city_codes[city]}.{wtype}.1."

inserat_sql = """
    INSERT INTO wg_gesucht.inserate (inserat_id, viertel, titel,
    miete_gesamt, miete_kalt, miete_sonstige, nebenkosten,
    kaution, abstandszahlung, verfuegbar, online_seit, stadt, frei_ab,
    frei_bis, adresse, groesse, mitbewohner, wohnungs_type, angaben,
    details) VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s, %s);
    """

images_sql = """
    INSERT INTO wg_gesucht.images_inserate (id, image)
    VALUES (%s,%s);
    """

inserat_ids_sql = """
    SELECT inserat_id FROM wg_gesucht.inserate
    WHERE stadt = %s
    AND wohnungs_type = %s;
    """

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

# get stored inserat_ids
cur.execute(inserat_ids_sql, (city, wtype,))
rows = cur.fetchall()
inserat_ids = [x[0] for x in rows] if len(rows) > 0 else []
print(f"Bisher {len(inserat_ids)} inserate für {city} und {wtype_d[wtype]}")

# login to wg-gesucht
session = requests.Session()
login_url = url + "ajax/api/Smp/api.php?action=login"
payload = {
    "login_email_username": config["WGGESUCHT"]["email"],
    "login_password": config["WGGESUCHT"]["pw"],
    "login_form_auto_login": "1",
    "display_language": "de",
}
login = session.post(
    login_url,
    json=payload
)

if not login.json():
    print("Could not log in with the given email and password")
    sys.exit(1)

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

def get_address(soup):
    """

    Retrieve address of wg

    :soup: BeautifulSoup object
    :return: {
        "city": ...,
        "street": ...,
        "house_number": ...,
        "plz": ...,
        "viertel": ...,
        "neighbourhood": ...

    """
    # get respective "div" element
    div_body = soup.find("div", class_="col-sm-8 card_body")
    div_text = soup.find("div", class_="col-xs-11").span.text
    # first item is wd detail so [1:]
    address_list = [
        " ".join(x.strip().split()) for x in div_text.split("|")[1:]
    ]
    # "/" durch " " ersetzen
    if "/" in address_list[1]:
        address_list[1] = address_list[1].replace("/", " ")
    # replace "Nähe" -> only produces ambiguous results
    if "nähe" in address_list[1].lower():
        address_list[1] = address_list[1].lower().replace(
            "nähe", ""
        ).strip()
    # format params for url
    params = address_list[0].split(" ")[0]+"+"+address_list[1].replace(" ", "+")
    # query nominatim search with given address details
    query_str = "https://nominatim.openstreetmap.org/search?" \
        f"q={params}&format=geojson&addressdetails=1&limit=1"
    print(query_str)
    r = http_get(query_str)
    # nominatim fairness rules -> wait 3 secs
    address_json = r.json()
    # get feature of FeatureCollection
    feat = address_json["features"][0]
    print(feat["properties"]["address"])

    # Stadtbergen != Augsburg
    if "city" in feat["properties"]["address"]:
        city = feat["properties"]["address"]["city"]
    elif "town" in feat["properties"]["address"]:
        city = feat["properties"]["address"]["town"]
    else:
        city = None

    # check if "house_number" availabe
    if "house_number" in feat["properties"]["address"]:
        house_number = feat["properties"]["address"]["house_number"]
    else:
        house_number = None

    # check if "neighbourhood" available
    if "neighbourhood" in feat["properties"]["address"]:
        neighbourhood = feat["properties"]["address"]["neighbourhood"]
    else:
        neighbourhood = None

    # check if "suburb" available
    if "suburb" in feat["properties"]["address"]:
        suburb = feat["properties"]["address"]["suburb"]
    else:
        suburb = None

    return {
        "city": city,
        "street": feat["properties"]["address"]["road"],
        "house_number": house_number,
        "plz": feat["properties"]["address"]["postcode"],
        "viertel": suburb,
        "neighbourhood": neighbourhood
    }

def get_insert_dt(soup):
    t_string = soup.find_all(
        "div", class_="col-sm-12 flex_space_between"
    )[-1].find_all("span")[-1].text
    # split at "online"
    online_since = t_string.split("Online: ")[1]
    # since when is offer online?
    if "Minute" in online_since:
        t = int(online_since.split(" Minute")[0])
        insert_datetime = datetime.now() - timedelta(minutes=t)
    elif "Stunde" in online_since:
        # t = int(online_since.split(": ")[1].split("Stunde")[0])
        t = int(online_since.split(" Stunde")[0])
        insert_datetime = datetime.now() - timedelta(hours=t)
    elif "Tag" in online_since:
        t = int(online_since.split(" Tag")[0])
        insert_datetime = datetime.now() - timedelta(days=t)
    else:
        insert_datetime = datetime.strptime(online_since, "%d.%m.%Y")

    return insert_datetime

def get_image(soup):
    img_url = soup.find("a").get("style").split("image: ")[1][4:-2]
    if "placeholder" in img_url:
        img_raw = None
    else:
        img_raw = http_get(img_url).content

    return img_raw

def get_ids(soup):
    """

    Retrieve availabe ids of adverts from main page
    Also generates info like realtor and ids
    :soup: soup of main page withe inserate (BeautifulSoup object)

    :returns: list of ids to available wgs

    """
    # wgs list
    wgs_list = soup.find_all("div", id=re.compile("^liste-details-ad"))
    # filter out "hidden" items
    wgs_list = [x for x in wgs_list if "hidden" not in x.get("id")]
    # filter out "Übernachtung"
    wgs_list = [
        x for x in wgs_list if not x.find("span", title="Übernachtung")
    ]
    # filter out "Tauschangebot"
    wgs_list = [
        x for x in wgs_list if not x.find("span", title="Tauschangebot")
    ]
    # get genral info of wg inserat from main page
    wg_items = [
        {
            "id": x.get("id").split("-")[-1],
            "url": url + x.find("a").get("href"),
            "realtor": x.find_all(
                "div", class_="col-sm-12 flex_space_between"
            )[-1].span.text,
            "insert_dt": get_insert_dt(x),
            "img_raw": get_image(x),
            "address": get_address(x)
        } for x in wgs_list
    ]

    return wg_items

def parse_wg(get_id_dict):
    wg_url = url + get_id_dict["id"] + ".html"
    soup = http_get_to_soup(wg_url)

    # get title
    main = soup.find("div", id="main_column")
    title = main.find("h1", class_=re.compile("^headline")).text.strip()

# read current counter
with open(script_path + "/wg_counter") as f:
    wg_counter = f.read()
print("wg-gesucht momentan bei " + wg_counter)

# get available pages
soup = http_get_to_soup(get_string + "0.html")
# find page_bar with numbers of pages
page_bar = soup.find_all("ul", class_="pagination pagination-sm")[0]
page_counter = int(page_bar.find_all("li")[-2].get_text().strip())
print(f"There are {page_counter} pages available")

# iterate lists of inserate
for i in range(int(wg_counter), page_counter):
    print(get_string + f"{i}.html")
    soup = http_get_to_soup(get_string + f"{i}.html")
    # get ids
    ids = get_ids(soup)

conn.close()

def get_sizes(self, soup):
    """

    Get size of wg
    (room, total)

    :soup: BeautifulSoup object
    :returns: TODO

    """

    # basic facts
    basic = soup.find("div", id="basic_facts_wrapper")
    # room size
    rent_wrapper = basic.find("div", id="rent_wrapper")
    # total size of living object
    size_all_raw = rent_wrapper.find(
        "div",
        class_="basic_facts_top_part"
    ).find("label", class_="amount").text.strip()
    # size_all not mandatory; change 'n.a.' to None
    if size_all_raw == "n.a.":
        size_all = None
    else:
        # strip m² from string
        size_all = size_all_raw.replace("m²", "")
    # wg type
    wg_desc = rent_wrapper.find(
        "label",
        class_="description").text.strip()
    # wg size room
    room_size_raw = rent_wrapper.find(
        "div",
        class_="basic_facts_bottom_part"
    ).find("label").text.strip()
    if room_size_raw == "n.a.":
        room_size = None
    else:
        room_size = room_size_raw.replace("m²", "")

    return {
        "size_all": size_all,
        "wg_type_all": wg_desc,
        "room_size": room_size
    }

def get_costs(self, soup):
    """

    Get costs of wg
    (miete, nebenkosten,
    sonstiges, kaution, abstandszahlung)

    :soup: BeautifulSoup object
    :returns: TODO

    """

    # basic facts
    basic = soup.find("div", id="basic_facts_wrapper")
    # costs
    graph_wrapper = basic.find("div", id="graph_wrapper")
    # rent; [1:] to remove first element
    cost_html_elements = graph_wrapper.find_all("div")[1:]
    # Indices: 0->sonstiges;1->Nebenkosten;2->Miete;3->Gesamt
    rent_list = []
    for e in cost_html_elements:
        text = e.text.strip().splitlines()[0].replace("€", "")
        rent_list.append(None if text == "n.a." else text)
    # kaution
    provision_eq = basic.find_all("div", class_="provision-equipment")
    provision = provision_eq[0].find("label").text.strip()
    # replace n.a. with None
    provision = None if provision == "n.a." else provision.replace("€", "")
    # Abstandszahlung
    abst = provision_eq[1].find("label").text.strip()
    abst = None if abst == "n.a." else abst.replace("€", "")

    return {
        "others": rent_list[0],
        "extra": rent_list[1],
        "rent": rent_list[2],
        "rent_all": rent_list[3],
        "deposit": provision,
        "transfer_fee": abst
    }

def get_angaben(self, soup):
    """

    Get angaben of wg
    (house type, wifi, furniture, parking, ...)

    :soup: BeautifulSoup object
    :returns: dict

    """

    # angaben zum objekt
    h3 = soup.find_all(
        "h3", class_="headline headline-detailed-view-panel-title"
    )
    try:
        angaben_row = [
            x.parent for x in h3 if x.text.strip() == "Angaben zum Objekt"
        ][0]
        # filter hidden div tags and div tags that have a span tag
        a_filtered = [
            x for x in angaben_row.find_all("div") if (
                x.span and not "aria-hidden" in x.span.attrs
            )
        ]
        # (house type, wifi, furniture, parking, ...)
        # create dict of items
        angaben_dict = dict([
            (x.span.attrs["class"][1].split("-", 1)[1],
             " ".join(x.text.replace("\n", "").strip().split())
             ) for x in a_filtered
        ])
    except IndexError:
        angaben_dict = None

    return angaben_dict

def get_roommates(self, soup):
    """

    Get roommates of wg

    :soup: BeautifulSoup object
    :returns: bytes

    """

    h3 = soup.find_all(
        "h3", class_="headline headline-detailed-view-panel-title"
    )
    details = [
        x for x in h3 if x.text.strip() == "WG-Details"
    ][0].parent.parent
    d_list = [
        " ".join(x.text.strip().replace("\n", "").split()) for x in
              details.find_all("li")
    ]
    # "Bewohneralter" is optional; so insert None at given position
    d_list = [(x if x != "" else None) for x in d_list]
    # check if last list element indicates that room is unavailable
    if "momentan vermietet" in details.find_all("li")[-1].text:
        r_all = int(re.findall(r'\d+', d_list[2])[0])
        roommates_bytes = bytes([r_all, 0, 0, 0])
    else:
        # parse roommates:
        # 4 Bytes [FF: All, FF: Women, FF: Men, FF: Diverse]
        r = soup.find_all("span", title=re.compile("WG"))[0]
        r_title = r.get("title")
        r_splits = r_title.split(" ")
        # format for roommates: 2er WG (1 Frau und 0 Männer und 0 Divers)
        r_all = int("".join([x for x in r_splits[0] if x.isdigit()]))
        comp = r_splits[2]
        r_comp = [int(re.findall(r'\d+', x)[0]) for x in comp.split(",")]

        roommates_bytes = bytes([r_all] + r_comp)

    return roommates_bytes

def get_details(self, soup):
    """

    Get details of wg
    (roommates, constellation, age, smoking, ...)

    :soup: BeautifulSoup object
    :returns: dict

    """

    # wg-details
    h3 = soup.find_all(
        "h3", class_="headline headline-detailed-view-panel-title"
    )
    details = [
        x for x in h3 if x.text.strip() == "WG-Details"
    ][0].parent.parent
    d_list = [
        " ".join(x.text.strip().replace("\n", "").split()) for x in
              details.find_all("li")
    ]
    # "Bewohneralter" is optional; so insert None at given position
    d_list = [(x if x != "" else None) for x in d_list]

    # "Rauchen" is optional so add None if not present
    if len(d_list) == 7:
        d_list.insert(4, None)


    return  {
        "wg_size": d_list[0],
        "wohnung_size": d_list[1],
        "roommate_age": d_list[3],
        "smoking": d_list[4],
        "wg_type": d_list[5],
        "languages": d_list[6],
        "looking_for": d_list[7]
    }


def get_availability( soup):
    """

    Get availability
    (from, until, online since)

    :soup: BeautifulSoup object
    :returns: avlblty_dict

    """

    if self.check_wg_available(soup):
        # availability
        h3 = soup.find_all(
            "h3", class_="headline headline-detailed-view-panel-title"
        )
        avlblty_row = [
            x for x in h3 if x.text.strip() == "Verfügbarkeit"
        ][0].parent.parent
        avlblty_p = avlblty_row.p.text.splitlines()
        avlblty_l = [
            x.strip() for x in avlblty_p if x.strip() != ""
        ]
        avlblty_dict = {
            "frei_ab": datetime.strptime(avlblty_l[1], "%d.%m.%Y").isoformat()
        }
        # "frei bis" is optional so check list length
        if len(avlblty_l) == 4:
            avlblty_dict["frei_bis"] = datetime.strptime(
                avlblty_l[3], "%d.%m.%Y"
            ).isoformat()
        else:
            avlblty_dict["frei_bis"] = None
        # since when is offer online?
        online_raw = soup.find_all("b", class_="noprint")
        online_since = online_raw[0].text.strip().split(": ")[1]
        # deduct time delta
        if "Minute" in online_since:
            # t = int(online_since.split(": ")[1].split(" Minute")[0])
            t = int(online_since.split(" Minute")[0])
            insert_datetime = datetime.now() - timedelta(minutes=t)
        elif "Stunde" in online_since:
            # t = int(online_since.split(": ")[1].split("Stunde")[0])
            t = int(online_since.split(" Stunde")[0])
            insert_datetime = datetime.now() - timedelta(hours=t)
        elif "Tag" in online_since:
            t = int(online_since.split(" Tag")[0])
            insert_datetime = datetime.now() - timedelta(days=t)
        else:
            insert_datetime = datetime.strptime(online_since, "%d.%m.%Y")

        avlblty_dict["insert_dt"] = insert_datetime
    else:
        # wg is opccupied
        avlblty_dict = {
            "frei_ab": None,
            "frei_bis": None,
            "insert_dt": None
        }

    return avlblty_dict

def check_wg_available(self, soup):
    """

    checks for h4 element in html with keyword "deaktiviert"

    :wg_url: url to wg advert (string)

    :returns: Bool if inserat is available
    """

    hfours = soup.find_all("h4", class_="headline alert-primary-headline")
    check = [x for x in hfours if "deaktiviert" in x.text]

    # 0 no alerts -> wg is available
    # > 1 alters -> wg is unavailable
    return len(check) == 0

def parse_wgs(self, wg_url):
    """

    Parse content of wg advert

    :wg_url: url to wg advert (string)

    :returns: combined dictionary with all info of inserat

    """
    inserat_id = self.get_id_of_url(wg_url)
    soup = self.http_get_to_soup(wg_url)
    if "https://www.wg-gesucht.de/cuba.html" in self.current_url:
        return 1
    else:
        return {
            "inserat_id": inserat_id,
            "title": self.get_title(soup),
            "address": self.get_address(soup),
            "sizes": self.get_sizes(soup),
            "costs": self.get_costs(soup),
            "angaben": self.get_angaben(soup),
            "details": self.get_details(soup),
            "availability": self.get_availability(soup),
            "check_available": self.check_wg_available(soup),
            "wg_images": self.get_wg_images(soup),
            "roommates": self.get_roommates(soup)
        }

def insert_into_inserate(self, parsed_wg):
    """

    Inserts parsed wg page into DB
    Used in conjunction with self.parse_wg

    :parsed_wg: items to fill into wg_gesucht.inserate (dict)

    """
    preped_l = [
        parsed_wg["inserat_id"],
        parsed_wg["address"]["viertel"] + "_" + self.stadt,
        parsed_wg["title"],
        parsed_wg["costs"]["rent_all"],
        parsed_wg["costs"]["rent"],
        parsed_wg["costs"]["others"],
        parsed_wg["costs"]["extra"],
        parsed_wg["costs"]["deposit"],
        parsed_wg["costs"]["transfer_fee"],
        parsed_wg["check_available"],
        parsed_wg["availability"]["insert_dt"],
        self.stadt,
        parsed_wg["availability"]["frei_ab"],
        parsed_wg["availability"]["frei_bis"],
        Json(parsed_wg["address"]),
        Json(parsed_wg["sizes"]),
        parsed_wg["roommates"],
        self.wtype,
        Json(parsed_wg["angaben"]),
        Json(parsed_wg["details"])
    ]
    self.execute_sql(self.cur,
                     self.inserat_sql,
                     preped_l)

    self.insert_into_images(
        parsed_wg["inserat_id"], parsed_wg["wg_images"]
    )

def insert_into_images(self, inserat_id, images_bytes):
    """

    Inserts bytes of image into DB
    Used in conjunction with self.get_wg_images

    :inserat_id: uid of inserat
    :images_bytes: bytes of image to fill into wg_gesucht.images_inserate

    """
    self.execute_sql(self.cur,
                     self.images_sql,
                     [inserat_id, images_bytes])

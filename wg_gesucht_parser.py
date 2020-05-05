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

# map icon-names to appropriate naming
angaben_map = {
    "mixed-buildings": "haustyp",
    "building": "etage",
    "bed": "einrichtung",
    "bath-bathtub": "sanitär",
    "wifi-alt": "internet",
    "fabric": "bodenbelag",
    "car": "parksituation",
    "bus": "entfernung_öpnv",
    "folder-closed": "sonstiges",
    "display": "tv",
    "leaf": "ökostrom",
    "fire": "heizung",
    "group": "wg_geeignet",
    "person-wheelchair": "barrierefrei"
}

# viertel mapper
viertel_map = {
    "uni": "Universitätsviertel_Augsburg",
    "uni-": "Universitätsviertel_Augsburg",
    "pfersee": "Pfersee_Augsburg",
    "rosenau-thelottviertel": "Pfersee_Augsburg",
    "thelottviertel": "Pfersee_Augsburg",
    "antonsviertel": "Antonsviertel_Augsburg",
    "inningen": "Inningen_Augsburg",
    "spickel-herrenbach": "Spickel-Herrenbach_Augsburg",
    "wolframviertel": "Spickel-Herrenbach_Augsburg",
    "spickel": "Spickel-Herrenbach_Augsburg",
    "herrenbach": "Spickel-Herrenbach_Augsburg",
    "hochfeld": "Hochfeld_Augsburg",
    "prinz karl viertel": "Hochfeld_Augsburg",
    "bergheim": "Bergheim_Augsburg",
    "innenstadt": "Innenstadt_Augsburg",
    "haunstetten-siebenbrunn": "Haunstetten-Siebenbrunn_Augsburg",
    "haunstetten nord": "Haunstetten-Siebenbrunn_Augsburg",
    "haunstetten süd": "Haunstetten-Siebenbrunn_Augsburg",
    "haunstetten": "Haunstetten-Siebenbrunn_Augsburg",
    "göggingen": "Göggingen_Augsburg",
    "hochzoll": "Hochzoll_Augsburg",
    "firnhaberau": "Firnhaberau_Augsburg",
    "universitätsviertel": "Universitätsviertel_Augsburg",
    "hammerschmiede": "Hammerschmiede_Augsburg",
    "kriegshaber": "Kriegshaber_Augsburg",
    "bärenkeller": "Bärenkeller_Augsburg",
    "lechhausen": "Lechhausen_Augsburg",
    "oberhausen": "Oberhausen_Augsburg",
    "rechts der wertach": "Oberhausen_Augsburg",
    "links der wertach": "Oberhausen_Augsburg",
    "man viertel": "Oberhausen_Augsburg",
    "zentrum": "Innenstadt_Augsburg",
    "textilviertel": "Spickel-Herrenbach_Augsburg",
    "jakobervorstadt": "Innenstadt_Augsburg",
    "bismarckviertel": "Innenstadt_Augsburg",
    "bismarkviertel": "Innenstadt_Augsburg",
    "bismarckviertel innenstadt": "Innenstadt_Augsburg",
    "mitte": "Innenstadt_Augsburg",
    "georgsviertel": "Innenstadt_Augsburg",
    "georgs- und kreuzviertel": "Innenstadt_Augsburg",
    "am schäfflerbach": "Innenstadt_Augsburg",
    "domviertel": "Innenstadt_Augsburg",
    "theaterviertel": "Innenstadt_Augsburg",
    "stadtjägerviertel": "Innenstadt_Augsburg",
    "stadtjäger": "Innenstadt_Augsburg",
    "jakobervorstadt süd": "Innenstadt_Augsburg",
    "jakobervorstadt nord": "Innenstadt_Augsburg",
    "bleiche und pfärrle": "Innenstadt_Augsburg",
    "bleich und pfärrle": "Innenstadt_Augsburg",
    "beethovenviertel": "Innenstadt_Augsburg",
    "heilig-kreuz-viertel": "Innenstadt_Augsburg",
    "augsburg": None,
    "bayern - augsburg": None,
    "königsbrunn": None,
    "stadtbergen": None,
    "biburg": None,
    "bergen": None,
    "pöttmes": None,
    "zusmarshausen": None,
    "aystetten": None,
    "neusäß": None,
    "steppach": None,
    "gersthofen": None,
    "friedberg-west": None,
    "friedberg": None,
    "deutschland": None,
    "": None
}

get_string = f"{url}{wtype_d[wtype]}-in-{city}.{city_codes[city]}.{wtype}.1."

inserat_sql = """
    INSERT INTO wg_gesucht.inserate (inserat_id, viertel, titel,
    miete_gesamt, miete_kalt, miete_sonstige, nebenkosten,
    kaution, abstandszahlung, verfuegbar, city, frei_ab,
    frei_bis, groesse, mitbewohner, wohnungs_type, angaben,
    details, online_seit, realtor, adress_str) VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """

images_sql = """
    INSERT INTO wg_gesucht.images_inserate (id, image)
    VALUES (%s,%s);
    """

inserat_ids_sql = """
    SELECT inserat_id FROM wg_gesucht.inserate
    WHERE wohnungs_type = %s;
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
cur.execute(inserat_ids_sql, (wtype,))
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

    Retrieve address string of advert

    :soup: BeautifulSoup object
    :return: adress_str (string)

    """
    # get respective "div" element
    div_body = soup.find("div", class_="col-sm-8 card_body")
    div_text = soup.find("div", class_="col-xs-11").span.text
    # first item is wd detail so [1:]
    address_l = [
        " ".join(x.strip().split()) for x in div_text.split("|")[1:]
    ]
    if "nähe" in address_l[1].lower():
        address_l[1] = address_l[1].lower().replace(
            "nähe", ""
        ).strip().title()

    if "nähe" in address_l[0].lower():
        address_l[0] = address_l[0].lower().replace(
            "nähe", ""
        ).strip().title()

    if "umgebung" in address_l[0].lower():
        address_l[0] = address_l[0].lower().replace(
            "umgebung", ""
        ).strip().title()

    if " - " in address_l[0]:
        address_l[0] = address_l[0].replace(" - ", " ").strip()

    if "/" in address_l[0]:
        address_l[0] = address_l[0].split("/")[0].strip()

    # manchmal schreiben User die PLZ mit. Hier entfernen
    address_l[0] = "".join([x for x in address_l[0] if not x.isdigit()])

    address_str = " ".join(address_l)
    viertel = " ".join(address_l[0].split()[1:]).lower()

    # königsbrunn und Stadtbergen liegen NICHT in Augsburg
    if viertel == "königsbrunn":
        address_city = "Königsbrunn"
    elif viertel == "stadtbergen":
        address_city = "Stadtbergen"
    elif viertel == "biburg":
        address_city = "Diedorf"
    elif viertel == "bergen":
        address_city = "Affing"
    elif viertel == "pöttmes":
        address_city = "Pöttmes"
    elif viertel == "zusmarshausen":
        address_city = "Zusmarshausen"
    elif viertel == "aystetten":
        address_city = "Aystetten"
    elif viertel == "neusäß":
        address_city = "Neusäß"
    elif viertel == "steppach":
        address_city = "Neusäß"
    elif viertel == "gersthofen":
        address_city = "Gersthofen"
    elif "friedberg" in viertel:
        address_city = "Friedberg"
    else:
        address_city = city

    # map the suburb name
    viertel_mapped = viertel_map[viertel]

    return {
        "address_str": address_str,
        "viertel": viertel_mapped,
        "city": address_city
    }

def get_insert_dt(soup):
    t_string = soup.find_all(
        "div", class_="col-sm-12 flex_space_between"
    )[-1].find_all("span")[-1].text.lower()
    # split at "online"
    online_since = t_string.split("online: ")[1]
    # since when is offer online?
    if "minute" in online_since:
        t = int(online_since.split(" minute")[0])
        insert_datetime = datetime.now() - timedelta(minutes=t)
    elif "stunde" in online_since:
        # t = int(online_since.split(": ")[1].split("stunde")[0])
        t = int(online_since.split(" stunde")[0])
        insert_datetime = datetime.now() - timedelta(hours=t)
    elif "tag" in online_since:
        t = int(online_since.split(" tag")[0])
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

def get_id(soup):
    inserat_id = soup.get("id").split("-")[-1]
    print(url + inserat_id + ".html")

    return inserat_id

def is_available(soup):
    if soup.find("span", class_="ribbon-deactivated"):
        available = False
    else:
        available = True

    return available

def get_details_from_main(soup):
    """

    retrieve availabe ids of adverts from main page
    also generates info like realtor and ids
    :soup: soup of main page withe inserate (beautifulsoup object)

    :returns: list of ids to available wgs

    """
    # wgs list
    wgs_list = soup.find_all("div", id=re.compile("^liste-details-ad"))
    # filter out "hidden" items
    wgs_list = [x for x in wgs_list if "hidden" not in x.get("id")]
    # filter out "übernachtung"
    wgs_list = [
        x for x in wgs_list if not x.find("span", title="übernachtung")
    ]
    # filter out "tauschangebot"
    wgs_list = [
        x for x in wgs_list if not x.find("span", title="tauschangebot")
    ]
    # filter out already parsed wgs
    wgs_list = [x for x in wgs_list if int(get_id(x)) not in inserat_ids]

    # get genral info of wg inserat from main page
    wg_items = [
        {
            "id": get_id(x),
            "url": url + x.find("a").get("href"),
            "realtor": x.find_all(
                "div", class_="col-sm-12 flex_space_between"
            )[-1].span.text,
            "insert_dt": get_insert_dt(x),
            "img_raw": get_image(x),
            "address": get_address(x),
            "available": is_available(x)
        } for x in wgs_list
    ]

    return wg_items

def parse_wg(details_d):
    soup = http_get_to_soup(details_d["url"])

    # get title
    main = soup.find("div", id="main_column")
    title = main.find("h1", class_=re.compile("^headline")).text.strip()
    details_d["title"] = title

    # get sizes
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

    details_d["sizes"] = {
        "size_all": size_all,
        "wg_type_all": wg_desc,
        "room_size": room_size
    }

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

    # add to dict
    details_d["costs"] = {
        "miete_sonstige": rent_list[0],
        "nebenkosten": rent_list[1],
        "miete_kalt": rent_list[2],
        "miete_gesamt": rent_list[3],
        "kaution": provision,
        "abstandszahlung": abst
    }

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
        angaben_d = dict([
            (x.span.attrs["class"][1].split("-", 1)[1],
             " ".join(x.text.replace("\n", "").strip().split())
             ) for x in a_filtered
        ])
    except IndexError:
        angaben_d = None

    # rename keys
    if angaben_d:
        keys = list(angaben_d.keys())
        for k in keys:
            angaben_d[angaben_map[k]] = angaben_d.pop(k)
        # check for "ökostrom"
        if "ökostrom" in angaben_d:
            angaben_d["ökostrom"] = True

    details_d["angaben"] = angaben_d

    # roommates
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

    # "Rauchen" is optional so add None if not present
    if len(d_list) == 7:
        d_list.insert(4, None)

    details_d["roommates_b"] = roommates_bytes
    details_d["details"] = {
        "wg_size": d_list[0],
        "wohnung_size": d_list[1],
        "roommate_age": d_list[3],
        "smoking": d_list[4],
        "wg_type": d_list[5],
        "languages": d_list[6],
        "looking_for": d_list[7]
    }

    # available: "frei_ab", "frei_bis"
    if details_d["available"]:
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
    else:
        # wg is opccupied
        avlblty_dict = {
            "frei_ab": None,
            "frei_bis": None
        }

    details_d["availability"] = avlblty_dict

    return details_d

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
    # get initial details from main listing
    main_details = get_details_from_main(soup)

    # filter out already parsed wgs
    main_details = [
        x for x in main_details if int(x["id"]) not in inserat_ids
    ]

    print("parsing WGS")
    for d in main_details:
        print("parsing WG " + d["url"])
        inserat_parsed = parse_wg(d)
        print({k: v for k, v in inserat_parsed.items() if k != "img_raw"})
        preped_l = [
            inserat_parsed["id"],
            inserat_parsed["address"]["viertel"],
            inserat_parsed["title"],
            inserat_parsed["costs"]["miete_gesamt"],
            inserat_parsed["costs"]["miete_kalt"],
            inserat_parsed["costs"]["miete_sonstige"],
            inserat_parsed["costs"]["nebenkosten"],
            inserat_parsed["costs"]["kaution"],
            inserat_parsed["costs"]["abstandszahlung"],
            inserat_parsed["available"],
            inserat_parsed["address"]["city"],
            inserat_parsed["availability"]["frei_ab"],
            inserat_parsed["availability"]["frei_bis"],
            Json(inserat_parsed["sizes"]),
            inserat_parsed["roommates_b"],
            wtype,
            Json(inserat_parsed["angaben"]),
            Json(inserat_parsed["details"]),
            inserat_parsed["insert_dt"],
            inserat_parsed["realtor"],
            inserat_parsed["address"]["address_str"]
        ]

        cur.execute(inserat_sql, preped_l)
        cur.execute(images_sql,
                    [inserat_parsed["id"], inserat_parsed["img_raw"]]
                    )

    with open(script_path + "/wg_counter", "w") as f:
        f.write(str(i))

cur.close()
conn.close()


from bs4 import BeautifulSoup
import configparser
from datetime import datetime, timedelta
import geopandas as gpd
import os
import pandas as pd
import psycopg2
from psycopg2.extras import Json
import random
import re
import requests
import sys
import time


class WohnungsMarkt:

    """

    Base class for wohnungsmarkt

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

    conn = psycopg2.connect("dbname=wohnungsmarkt_db user=sepp")
    config = configparser.ConfigParser()

    def __init__(self):
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

    def http_get(self, url):
        """
        Simple Helper method to HTTP GET urls and validate

        :url: the url to get (string) -> default get_string
        :returns: requests Response object
        """
        try:
            r = requests.get(url)
            if r.status_code == 200:
                return r
            else:
                raise requests.HTTPError(
                    f"Request failed with status_code {r.status_code}"
                )
        except requests.ConnectionError:
            raise url + " probably offline!"

    def execute_sql(self, cursor, sql_str, params):
        """
        General Method to execute SQL statements for
        a given Database

        :cursor: cursor for conn
        :sql_str: sql_str to be executed
        :params: params for sql_str (tuple)
        """

        cursor.execute(sql_str, params)

class WgGesucht(WohnungsMarkt):

    """Docstring for WgGesucht. """
    url = "https://www.wg-gesucht.de/"

    inserat_sql = """
        INSERT INTO wg_gesucht.inserate (inserat_id, viertel, titel,
        miete_gesamt, miete_kalt, miete_sonstige, nebenkosten,
        kaution, abstandszahlung, verfuegbar, insert_date, stadt, frei_ab,
        frei_bis, adresse, groesse, mitbewohner, wohnungs_type, angaben) VALUES
        (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
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

    session = requests.Session()
    # store current page content
    p_cnt = 0
    soup = None
    urls = []
    current_url = None

    def __init__(self, wtype, stadt):
        """

        :wtype = {
            1: "wg",
            2: "1-zimmer-wohnung",
            3: "wohnung",
            4: "haus"
        }
        :stadt: welche Stadt? (string)

        """
        super().__init__()
        self.wtype = wtype
        self.stadt = stadt
        self.config.read(
            os.path.dirname(os.path.realpath(__file__)) + "/cfg.ini"
        )
        self.viertel = self.__get_viertel(stadt)
        self.roads = self.__get_roads(stadt)
        self.inserat_ids = self.__get_inserat_ids(stadt, wtype)
        self.__sign_in()
        self.get_string = self.url + self.wtype_dict[wtype] \
        + "-in-" + stadt + "." + self.city_codes[stadt] + ".0.0."

    def __get_inserat_ids(self, city, wtype):
        """

        Loads currently stored inserat_ids
        for given wtype and city

        :city: city to filter inserate (string)
        :wtype: wtype to filter inserate (string)

        :returns: list of inserat_ids

        """
        self.cur.execute(self.inserat_ids_sql, (city, wtype,))
        rows = self.cur.fetchall()

        return [x[0] for x in rows] if len(rows) > 0 else []

    def __get_viertel(self, city):
        """

        Loads administrative areas of city
        areas are stored as geojson files

        viertel are stored as attribute

        :city: city to build geodatframe from (string)

        """
        # os.walk creates generator
        # ("root", ["dirs"], ["files"])
        root, _, files = list(
            os.walk(os.path.dirname(os.path.realpath(__file__)) \
                    + "/geojson/" + city.lower() + "/viertel")
        )[0]
        # create GeoDataFrame from all available "viertel"
        gdf = gpd.GeoDataFrame(
            pd.concat([
                gpd.read_file(root + "/" + file) for file in files
            ])
        )
        return gdf

    def __get_roads(self, city):
        """

        Loads roads of city

        :city: city to build geodatframe from (string)

        """
        # create GeoDataFrame from "city"_roads.geojson file
        gdf = gpd.read_file(
            os.path.dirname(os.path.realpath(__file__)) + "/geojson/" \
            + city.lower() + "/" + city.lower() + "_roads.geojson"
        )

        return gdf

    def __sign_in(self):
        """

        sign in to wg_gesucht.de with default credentials from cfg.ini
        sets requests session

        """

        payload = {
            "login_email_username": self.config["WGGESUCHT"]["email"],
            "login_password": self.config["WGGESUCHT"]["pw"],
            "login_form_auto_login": "1",
            "display_language": "de",
        }

        try:
            login = self.session.post(
                "https://www.wg-gesucht.de/ajax/api/Smp/api.php?action=login",
                json=payload,
            )
        except requests.exceptions.Timeout:
            print("Timed out trying to log in")
        except requests.exceptions.ConnectionError:
            print("Could not connect to internet")

        if not login.json():
            print("Could not log in with the given email and password")
            sys.exit(1)

    def http_get_to_soup(self, url):
        """
        HTTP get request to the desired url
        transforms text-content to BeautifulSoup object

        :url: http-string (string)
        :returns: BeautifulSoup object
        """

        r = self.http_get(url)
        self.current_url = r.url

        # only allow content type "text/html"
        if "text/html" in r.headers["Content-Type"]:
            soup = BeautifulSoup(r.text, "lxml")
            self.soup = soup
            return soup
        else:
            print(f"Expected Content Type text/html, but got \
                  {r.headers['Content-Type']} instead")

    def nominatim_request(self, string_l):
        """

        searches nominatim DB for string_l
        :string_l list of string that contain address data
        :returns: geojson

        """
        nominatim_str = "https://nominatim.openstreetmap.org/search?q="
        address_data = self.stadt + "+" + "+".join(string_l)
        form = "&format=geojson"
        r = self.http_get(nominatim_str + address_data + form)
        json = r.json()
        # parse json content
        # requests should return a FeatureCollection as type
        if json["type"] == "FeatureCollection":
            # FC should only have one feature if there is a
            # housenumber provided
            if len(json["features"]) == 1:
                display_n = json["features"][0]["properties"]["display_name"]
                # viertel is the 4th item in the string
                viertel = display_n.split(", ")[3]
        else:
            print("wrong format")

        return viertel

    def get_page_counter(self):
        """

        gets count of pages available

        :returns: pg_count (int)

        """

        # reset page counter
        soup = self.http_get_to_soup(self.get_string + "0.html")
        if "https://www.wg-gesucht.de/cuba.html" in self.current_url:
            return 1
        else:
            # find page_bar with numbers of pages
            page_bar = soup.find_all(
                "ul", class_="pagination pagination-sm"
            )[0]
            page_counter = int(
                page_bar.find_all("li")[-2].get_text().strip()
            )
            self.p_cnt = page_counter
            print(f"There are {page_counter} pages available")

            return page_counter

    def get_id_of_url(self, wg_url):
        """

        parse id of wg_url
        :wg_url: url of wg_gesucht (string)

        :returns: uid of inserat (int)
        """

        inserat_id = int(wg_url.split(".")[-2])

        return inserat_id

    def get_urls(self, p_cnt):
        """

        Retrieve availabe urls of adverts from main page
        :p_cnt: p_cnt (int)

        :returns: list of urls to available wgs

        """

        # get main offer listing
        soup = self.http_get_to_soup(self.get_string + str(p_cnt) + ".html")
        # wgs list from "main column"
        wgs_body = soup.find("div", id="main_column").table.tbody.find_all("tr")
        # filter out AirBnb offers
        wgs = [
            x.find("td", class_="ang_spalte_datum row_click") for x in wgs_body
        ]
        # filter out None Types
        wgs_filter = list(filter(None, wgs))
        urls = ["https://www.wg-gesucht.de/" + x.a["href"] for x in wgs_filter]
        self.urls = urls

        return urls

    def get_title(self, soup):
        """

        Get Title of wg offer
        :soup: BeautifulSoup object
        :returns: title (string)

        """

        main = soup.find("div", id="main_column")
        # get title
        title = main.find("h1", class_=re.compile("^headline")).text.strip()

        return title


    def get_wg_images(self, soup):
        """

        Get wg image

        :soup: BeautifulSoup object
        :returns: TODO

        """
        # TODO bei request wird nur 1 Bild angezeigt -> Versuchen alle Bilder
        # zu extrahieren!
        # Als Uebergangsloesung wird Bild aus Header genommen
        img_link = soup.head.find("link", rel="image_src").get("href")
        # wg images are optional
        # https://img.wg-gesucht.de/ is the default img url
        if img_link == "https://img.wg-gesucht.de/":
            img_raw = None
        else:
            img_raw = self.http_get(img_link).content

        return img_raw

    def get_address(self, soup):
        """

        Retrieve address of wg
        address is hidden in link ("a")

        :soup: BeautifulSoup object
        :returns: address dictionary
                 {
                "street": ...,
                "house_number": ...,
                "plz": ...,
                "viertel": ...
                }

        """

        # get address; hidden in link ("a")
        address = soup.find("div", class_="col-sm-4 mb10").text.strip()
        # parse for "newline" and split
        address_list = [
            x.strip() for x in address.splitlines() if x.strip() != ""
        ]
        # street and house number
        # extract house number; house number is not mandatory!
        if any(i.isdigit() for i in address_list[1]):
            house_number = address_list[1].split(" ")[-1]
            # because it is not known how many elements a street contains
            # it is necessary to split first
            street = " ".join(address_list[1].split(" ")[:-1])
        else:
            house_number = None
            street = " ".join(address_list[1].split(" "))
        plz = address_list[2].split(" ")[0]
        viertel = " ".join(address_list[2].split(" ")[2:])

        return {
            "street": street,
            "house_number": house_number,
            "plz": plz,
            "viertel": viertel
        }

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

        # parse roommates:
        # 4 Bytes [FF: All, FF: Women, FF: Men, FF: Diverse]
        r = soup.find_all("span", title=re.compile("WG"))[0]
        r_title = r.get("title")
        r_splits = r_title.split(" ")
        # format for roommates: 2er WG (1 Frau und 0 Männer und 0 Divers)
        r_all = int("".join([x for x in r_splits[0] if x.isdigit()]))
        comp = r_splits[2]
        r_comp = [int(re.findall(r'\d+', x)[0]) for x in comp.split(",")]
        # r_w = int(r_splits[2].strip("("))
        # r_m = int(r_splits[5])
        # r_d = int(r_splits[8])

        roommates_bytes = bytes(r_comp)

        return  {
            "wg_size": d_list[0],
            "wohnung_size": d_list[1],
            "roommates": d_list[2],
            "roommate_age": d_list[3],
            "smoking": d_list[4],
            "wg_type": d_list[5],
            "languages": d_list[6],
            "looking_for": d_list[7],
            "roommates_bytes": roommates_bytes
        }

    def get_availability(self, soup):
        """

        Get availability
        (from, until, online since)

        :soup: BeautifulSoup object
        :returns: avlblty_dict

        """

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
                "wg_images": self.get_wg_images(soup)
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
            parsed_wg["details"]["roommates_bytes"],
            self.wtype,
            Json(parsed_wg["angaben"])
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

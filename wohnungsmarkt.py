from bs4 import BeautifulSoup
import re
import requests


class WohnungsMarkt:

    """

    Base class for wohnungsmarkt

    """

    self.wtype_dict = {
        1: "wg-zimmer",
        2: "1-zimmer-wohnungen",
        3: "wohnungen",
        4: "haeuser"
    }

    self.city_codes = {
        "Augsburg": "2"
    }

    def http_get(self, url):
        """
        Simple Helper method to HTTP GET urls and validate

        :url: the url to get (string) -> default get_string
        :returns: requests Response object
        """

        r = requests.get(url)
        if r.status_code == 200:
            return r
        else:
            raise requests.HTTPError(f"Request failed with status_code {r.status_code}")


class WgGesucht(WohnungsMarkt):

    """Docstring for WgGesucht. """
    url = "https://www.wg-gesucht.de/"

    def __init__(self, wtype, stadt):
        """

        :wtype = {
            1: "wg",
            2: "1-zimmer-wohnung",
            3: "wohnung",
            4: haus
        }
        :stadt: welche Stadt? (string)

        """
        # super().__init__()
        self.wtype = wtype
        self.stadt = stadt
        self.p_cnt = 0
        # store current page content
        self.soup = None
        self.urls = []
        self.get_string = self.url + self.wtype_dict[wtype] + "-in-" + stadt + "." \
        + self.city_codes[stadt] + ".0.0." + f"{self.p_cnt}" + ".html"

    def http_get_to_soup(self, url):
        """
        HTTP get request to the desired url
        transforms text-content to BeautifulSoup object

        :url: http-string (string)
        :returns: BeautifulSoup object
        """

        r = self.http_get(url)
        # only allow content type "text/html"
        if r.headers["Content-Type"] == "text/html":
            soup = BeautifulSoup(r.text)
            self.soup = soup
            return soup
        else:
            print(f"Expected Content Type text/html, but got \
                  {r.headers['Content-Type']} instead")

    def get_page_counter(self, soup):
        """

        gets count of pages available

        :returns: pg_count (int)

        """

        # find page_bar with numbers of pages
        page_bar = soup.find_all("ul", class_="pagination pagination-sm")[0]
        page_counter = page_bar.find_all("li")[-2].get_text().strip()
        self.p_cnt = page_counter
        return page_counter

    def get_urls(self, soup):
        """

        Retrieve availabe urls of adverts from main page
        :soup: BeautifulSoup object

        :returns: list of urls to available wgs

        """
        # wgs list from "main column"
        wgs_body = soup.find("div", id="main_column").table.tbody.find_all("tr")
        # filter out AirBnb offers
        wgs = [x.find("td", class_="ang_spalte_datum row_click") for x in wgs_body]
        # filter out None Types
        wgs_filter = list(filter(None, wgs))
        self.urls = ["https://www." + x.a["href"] for x in wgs_filter]

        return urls

    def parse_wgs(self, wg_url):
        """

        Parse content of wg advert

        :wg_url: url to wg advert (string)


        :returns: TODO

        """

        soup = self.http_get_to_soup(wg_url)
        # TODO kontakte mit reinnehmen??
        # Problem: Name und Handynr werden NICHT im Klartext angezeigt
        # # get contact info
        # contact_panel = soup.find_all(
        #     "div",
        #     class_="panel panel-rhs-default rhs_contact_information hidden-sm"
        # )
        # # get contact picture
        # pic = contact_panel.find(
        #     "div", class_="profile_image_dav cursor-pointer"
        # )
        # # pic available
        # if pic is not None:
        #     pic_url = pic["data-featherlight"]
        #     # get image
        #     r = requests.get(pic_url)
        #     pic_kontakt_bytea = r.content
        # else:
        #     pic_kontakt_bytea = None
        # # get contact name / alias

        # # member since
        # member_since = list(
        #     contact_panel.find("div", class_="col-md-8")
        # )[4].strip()

        main = soup.find("div", id="main_column")
        # get title
        title = main.find("h1", class_=re.compile("^headline")).text.strip()

        # get images of wg-object
        # TODO bei request wird nur 1 Bild angezeigt -> Versuchen alle Bilder
        # zu extrahieren!
        # Als Uebergangsloesung wird Bild aus Header genommen
        img_link = soup.head.find("link", rel="image_src").get("href")
        img_raw = self.http_get(img_link).content

        # get address; hidden in link ("a")
        adress_strings = soup.find("div", class_"col-sm-4 mb10").text.strip()
        # parse for "newline" and split
        adress_list = [
            x.strip() for x in adress.split("\n") if x.strip() is not ""
        ]
        # street and house number
        # extract house number; house number is not mandatory!
        if any(i.isdigit() for i in adress_list[1]):
            house_number = adress_list[1].split(" ")[-1]
            # because it is not known how many elements a street contains
            # it is necessary to split first
            street = " ".join(adress_list[1].split(" ")[:-1])
        else:
            house_number = None
            street = " ".join(adress_list[1].split(" "))
        plz = adress_list[2].split(" ")[0]
        viertel = " ".join(adress_list[2].split(" ")[2:])

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
        room_size_raw = rent_wrapper2.find(
            "div",
            class_="basic_facts_bottom_part"
        ).find("label").text.strip()
        if room_size_raw == "n.a.":
            room_size = None
        else:
            room_size = room_size_raw.replace("m²", "")

        # costs
        graph_wrapper = basic.find("div", id="graph_wrapper")
        # rent; [1:] to remove first element
        cost_html_elements = graph_wrapper.find_all("div")[1:]
        # Indices: 0->sonstiges;1->Nebenkosten;2->Miete;3->Gesamt
        rent_list = []
        for e in cost_html_elements:
            text = e.text.strip().split("\n")[0].replace("€", "")
            rent_list.append(None if text == "n.a." else text)
        # kaution
        provision_eq = basic.find_all("div", class_="provision-equipment")
        provision = provision_eq[0].find("label").text.strip()
        # replace n.a. with None
        provision = None if provision == "n.a." else provision.replace("€", "")
        # Abstandszahlung
        abst = provision_eq[1].find("label").text.strip()
        abst = None if abst == "n.a." else abst.replace("€", "")

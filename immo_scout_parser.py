import os
import re
import sys
import psycopg2
import requests
import configparser

from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import TooManyRedirects, HTTPError


assert (len(sys.argv) == 3), "Too few/many arguments"
wtype = sys.argv[1]
city = sys.argv[2].lower()

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

# connect to db
conn = psycopg2.connect(
    dbname=config["DATABASE"]["dbname"],
    user=config["DATABASE"]["user"],
    host=config["DATABASE"]["host"]
)

conn.autocommit = True
cur = conn.cursor()

# main url
url = "https://www.immobilienscout24.de/"

# expose-url
expo_url = url + "expose/"

wtype_d = {
    "0": "wohnung-mieten"
}

inserat_select_sql = """
    SELECT inserat_id FROM immoscout.inserate;
    """

inserat_insert_sql = """
    INSERT INTO immoscout.inserate(
    inserat_id, titel, miete_gesamt, miete_kalt, miete_heizkosten,
    nebenkosten, kaution, verfuegbar, frei_ab, wohnungs_type, realtor,
    kosten_stellplatz, plz, haustiere, einbaukueche, aufzug,
    balkon_terrasse, gaeste_wc, garten, keller, barrierefrei, wg_geeignet, wbs,
    ausstattung_qualitaet, etage, etage_all, badezimmer, zustand,
    modernisierung_jahr, garage_stellplatz, garage_stellplatz_cnt,
    energietraeger, heizungsart, energieausweis, energieausweis_art,
    energieeffizienzklasse, baujahr_gebaeude, zimmer_anzahl,
    schlafzimmer, wohnflaeche, nutzflaeche, schufa_auskunft,
    online_besichtigung, such_str) VALUES
    (%(data_id)s,%(titel)s,%(miete_gesamt)s,%(miete_kalt)s,
    %(miete_heizkosten)s,%(nebenkosten)s,%(kaution)s,%(verfuegbar)s,
    %(frei_ab)s,%(wohnungs_type)s,%(realtor)s,
    %(kosten_stellplatz)s,%(plz)s,%(haustiere)s,%(einbaukueche)s,
    %(aufzug)s,%(balkon_terrasse)s,%(gaeste_wc)s,%(garten)s, %(keller)s,
    %(barrierefrei)s,%(wg_geeignet)s,%(wbs)s,%(ausstattung_qualitaet)s,
    %(etage)s,%(etage_all)s,%(badezimmer)s,%(zustand)s,%(modernisierung_jahr)s,
    %(garage_stellplatz)s,%(garage_stellplatz_cnt)s,%(energietraeger)s,
    %(heizungsart)s,%(energieausweis)s,%(energieausweis_art)s,
    %(energieeffizienzklasse)s,%(baujahr_gebaeude)s,
    %(zimmer_anzahl)s,%(schlafzimmer)s,%(wohnflaeche)s,%(nutzflaeche)s,
    %(schufa_auskunft)s,%(online_besichtigung)s,%(such_str)s);
    """

images_insert_sql = """
    INSERT INTO immoscout.images_inserate(
    image, id)
    VALUES (%s, %s)
    """

haustier_d = {
    "Haustiere  Nach Vereinbarung": 1,
    "Haustiere  Ja": 2,
    "Haustiere  Nein": 3
}
wohnungs_type_d = {
    "Souterrain": 1,
    "Erdgeschosswohnung": 2,
    "Hochparterre": 3,
    "Etagenwohnung": 4,
    "Loft": 5,
    "Maisonette": 6,
    "Terrassenwohnung": 7,
    "Penthouse": 8,
    "Dachgeschoss": 9,
    "Sonstige": 10
}

energieausweis_d = {
    "Energieausweis liegt vor" : 1,
    "Energieausweis liegt zur Besichtigung vor": 2,
    "Energieausweis laut Gesetz nicht erforderlich": 3
}

energieausweisart_d = {
    "Energieausweistyp Bedarfsausweis": 1,
    "Energieausweistyp Verbrauchsausweis": 2
}


garage_d = {
    "Außenstellplatz": 1,
    "Außenstellplätze": 1,
    "Carport": 2,
    "Duplex-Stellplatz": 3,
    "Garage": 4,
    "Garagen": 4,
    "Parkhaus-Stellplatz": 5,
    "Tiefgaragen-Stellplatz": 6,
    "Tiefgaragen-Stellplätze": 6,
    "Stellplatz": 7,
    "Stellplätze": 7
}

zustand_d = {
    "Keine Angabe": 1,
    "Erstbezug": 2,
    "Erstbezug nach Sanierung": 3,
    "Neuwertig": 4,
    "Saniert": 5,
    "Modernisiert": 6,
    "Vollständig renoviert": 7,
    "Gepflegt": 8,
    "Renovierungsbedürftig": 9,
    "Nach Vereinbarung": 10,
    "Abbruchreif": 11
}

ausstattung_qualitaet_d = {
    "Keine Angabe": 1,
    "Luxus": 2,
    "Gehobene Qualität": 3,
    "Normale Qualität": 4,
    "Einfache Qualität": 5
}

heizungsart_d = {
    "Keine Angabe": 1,
    "Blockheizkraftwerke": 2,
    "Elektro-Heizung": 3,
    "Etagenheizung": 4,
    "Fernwärme": 5,
    "Fußbodenheizung": 6,
    "Gas-Heizung": 7,
    "Holz-Pelletheizung": 8,
    "Nachtspeicheröfen": 9,
    "Ofenheizung": 10,
    "Öl-Heizung": 11,
    "Solar-Heizung": 12,
    "Wärmepumpe": 13,
    "Zentralheizung": 14
}

energietraeger_d = {
    "Keine Angabe": 1,
    "Erdwärme": 2,
    "Solar": 3,
    "Holzpellets": 4,
    "Gas": 5,
    "Öl": 6,
    "Fernwärme": 7,
    "Strom": 8,
    "Kohle": 9,
    "Erdgas leicht": 10,
    "Erdgas schwer": 11,
    "Flüssiggas": 12,
    "Fernwärme-Dampf": 13,
    "Holz": 14,
    "Holz-Hackschnitzel": 15,
    "Kohle-Koks": 16,
    "Nahwärme": 17,
    "Wärmelieferung": 18,
    "Bioenergie": 19,
    "Windenergie": 20,
    "Wasserenergie": 21,
    "Umweltwärme": 22,
    "KWK fossil": 23,
    "KWK erneuerbar": 24,
    "KWK regenerativ": 25,
    "KWK bio": 26
}

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

def get_image(soup):
    gallery_box =  soup.find("div", "is24-expose-gallery-box")
    child = [x for x in list(gallery_box.children) if x != " "][0]
    if "no-header-gallery-image" in child.get("class"):
        img_raw = None
    else:
        img_url = child.find("img").get("src")
        try:
            img_raw = http_get(img_url).content
        except HTTPError:
            img_raw = None

    return img_raw

def get_date(date_s):
    # manchmal werden zwei Datum angegeben
    if "/" in date_s:
        date_s = date_s.split("/")[0].strip()
    date_patterns = ["%d.%m.%y", "%d.%m.%Y", "%Y-%m-%d"]
    for pattern in date_patterns:
        try:
            return datetime.strptime(date_s, pattern).date()
        except ValueError:
            pass
    print(f"Unbekanntes Date Format: {date_s}")
    sys.exit(0)

def parse_expose(soup):
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
        adress_str = " ".join(adress_str.split())
        adress_str = adress_str.strip()
    else:
        plz = adress_parts[1].split()[0]
        # get rid of plz
        adress_main = " ".join(adress_parts[1].strip().split())
        adress_combed = " ".join(
            [adress_main, adress_parts[0].strip()]
        )
        adress_str = adress_combed.replace(",", "")

    # Kosten
    kosten_h = [x for x in soup.find_all("h4") if x.text == " Kosten "][0]
    kosten_block = list(kosten_h.next_elements)[2]
    kosten_l = [x.text for x in kosten_block.find_all("dl")]

    # regex string to extract floats
    # https://stackoverflow.com/questions/4703390/how-to-extract-a-floating-number-from-a-string
    reg = r"\d*\.*\d+"

    # string in korrekte floats formatieren
    # 1.382,75 -> 1382.75
    floater = lambda a : a.replace(".", "").replace(",", ".")

    # Kaltmiete
    miete_kalt = [x for x in kosten_l if "Kaltmiete" in x][0]
    miete_kalt = re.findall(reg, floater(miete_kalt))[0]

    # Nebenkosten
    try:
        nebenkosten = [x for x in kosten_l if "Nebenkosten" in x][0]
        nebenkosten = re.findall(reg, floater(nebenkosten))[0]
    except IndexError:
        nebenkosten = None

    # Heizkosten
    try:
        miete_heizkosten = [x for x in kosten_l if "Heizkosten" in x][0]
        miete_heizkosten = re.findall(reg, floater(miete_heizkosten))[0]
    except IndexError:
        miete_heizkosten = None

    # Kaution
    try:
        kaution = [x for x in kosten_l if "Kaution" in x][0]
        kaution = re.findall(reg, floater(kaution))[0]
    except IndexError:
        kaution = None

    # Kosten Stellplatz
    try:
        kosten_stellplatz = [x for x in kosten_l if "Miete für Garage" in x][0]
        kosten_stellplatz = re.findall(reg, floater(kosten_stellplatz))[0]
    except IndexError:
        kosten_stellplatz = None

    # Miete gesamt
    try:
        miete_gesamt = [x for x in kosten_l if "Gesamtmiete" in x][0]
        miete_gesamt = re.findall(reg, floater(miete_gesamt))[0]
    except IndexError:
        miete_gesamt = None

    # details
    details = soup.find_all(
        "div", class_="criteriagroup criteria-group--two-columns")[0]
    details_l = [x.text for x in details.find_all("dl")]

    # Filter out Schufaauskunft, Internetverfügbarkeit
    exc_strs = (" Internet  Verfügbarkeit prüfen  ")
    details_l = [x for x in details_l if x not in exc_strs]

    # Bonitätsauskunft erforderlich?
    try:
        bon_item = [x for x in details_l if "Bonitätsauskunft" in x][0]
        bon_type_ix = details_l.index(bon_item)
        schufa_auskunft = "erforderlich" in details_l.pop(bon_type_ix)
    except IndexError:
        schufa_auskunft = False

    # wohnungs_type
    try:
        w_item = [x for x in details_l if "Typ" in x][0]
        w_type_ix = details_l.index(w_item)
        wohnungs_type = details_l.pop(w_type_ix).split("Typ")[1].strip()
        wohnungs_type = wohnungs_type_d[wohnungs_type]
    except IndexError:
        wohnungs_type = None

    # Etage
    try:
        e_item = [x for x in details_l if " Etage " in x][0]
        e_item_ix = details_l.index(e_item)
        e_item = details_l.pop(e_item_ix)
        # wenn Etagen Gesamtzahl angegeben wird dan "1 von 3"
        if "von" in e_item:
            etage_split = e_item.split("von")
            etage_all = etage_split[1].strip()
            etage = [x for x in etage_split[0] if x.isdigit()][0]
        else:
            etage = e_item.split("Etage")[1].strip()
            etage_all = None
    except IndexError:
        etage = None
        etage_all = None

    # groesse
    # Wohnfläche
    g_item = [x for x in details_l if "Wohnfläche" in x][0]
    g_item_ix = details_l.index(g_item)
    g_item = details_l.pop(g_item_ix)
    wohnflaeche = re.findall(reg, floater(g_item))[0]

    # Nutzfläche
    if any(["Nutzfläche" in x for x in details_l]):
        n_item = [x for x in details_l if "Nutzfläche" in x][0]
        n_item_ix = details_l.index(n_item)
        n_item = details_l.pop(n_item_ix)
        nutzflaeche = re.findall(reg, floater(n_item))[0]
    else:
        nutzflaeche = None

    # frei ab
    if any(["Bezugsfrei" in x for x in details_l]):
        f_item = [x for x in details_l if "Bezugsfrei" in x][0]
        f_item_ix = details_l.index(f_item)
        date_str = details_l.pop(f_item_ix)
        date_str = date_str.split("Bezugsfrei ab")[1].strip()
        if re.search(r"[sS].+t|[vV]ereinbarung", date_str):
            frei_ab = datetime.now().date()
        else:
            date_str = re.search(r"\d{2,4}?[/.,-]\d{2}[/.,-]\d{2,4}", date_str)
            if date_str:
                frei_ab = get_date(date_str.group())
            else:
                frei_ab = datetime.now().date()
    else:
        frei_ab = datetime.now().date()

    # Zimmer
    # Schlafzimmer
    if any(["Schlafzimmer" in x for x in details_l]):
        s_item = [x for x in details_l if "Schlafzimmer" in x][0]
        s_item_ix = details_l.index(s_item)
        s_item = details_l.pop(s_item_ix)
        schlafzimmer = re.findall(reg, floater(s_item))[0]
    else:
        schlafzimmer = None

    # Badezimmer
    if any(["Badezimmer" in x for x in details_l]):
        b_item = [x for x in details_l if "Badezimmer" in x][0]
        b_item_ix = details_l.index(b_item)
        b_item = details_l.pop(b_item_ix)
        badezimmer = re.findall(reg, floater(b_item))[0]
    else:
        badezimmer = None

    # Zimmer gesamt
    z_item = [x for x in details_l if "Zimmer" in x][0]
    z_item_ix = details_l.index(z_item)
    z_item = details_l.pop(z_item_ix)
    zimmer_anzahl = re.findall(reg, floater(z_item))[0]

    # Haustiere
    if any(["Haustier" in x for x in details_l]):
        h_item = [x for x in details_l if "Haustier" in x][0]
        h_item_ix = details_l.index(h_item)
        h_item = details_l.pop(h_item_ix).strip()
        haustiere = haustier_d[h_item]
    else:
        # 0 = keine Angabe
        haustiere = 0

    # Garage
    if any(["Garage" in x for x in details_l]):
        garage_item = [x for x in details_l if "Garage" in x][0]
        garage_item_ix = details_l.index(garage_item)
        garage_item = details_l.pop(garage_item_ix).strip()
        # Anzahl an Garagenplätzen ist optional
        try:
            garage_stellplatz_cnt = re.findall(reg, garage_item)[0]
        except IndexError:
            garage_stellplatz_cnt = None
        garage_stellplatz = garage_d[garage_item.split()[-1]]
    else:
        # 0 = keine Angabe
        garage_stellplatz = 0
        garage_stellplatz_cnt = None

    assert len(details_l) == 0

    #################### Ausstattung #########################################
    ausstattung_l = soup.find(
        "div", class_="criteriagroup boolean-listing padding-top-l"
    )

    if ausstattung_l:
        ausstattung_l = ausstattung_l.find_all(
            "span", class_=re.compile("^palm-hide")
        )
        ausstattung_l = [x.text for x in ausstattung_l]

        # Onlinebesichtigung
        if any(["Online-Besichtigung möglich" in x for x in ausstattung_l]):
            online_besichtigung = True
            ausstattung_l.pop(ausstattung_l.index("Online-Besichtigung möglich"))
        else:
            online_besichtigung = None

        # Einbauküche
        if any(["Einbauküche" in x for x in ausstattung_l]):
            einbaukueche = True
            ausstattung_l.pop(ausstattung_l.index("Einbauküche"))
        else:
            einbaukueche = None

        # Balkon Terrasse
        if any(["Balkon/ Terrasse" in x for x in ausstattung_l]):
            balkon_terrasse = True
            ausstattung_l.pop(ausstattung_l.index("Balkon/ Terrasse"))
        else:
            balkon_terrasse = None

        # Keller
        if any(["Keller" in x for x in ausstattung_l]):
            keller = True
            ausstattung_l.pop(ausstattung_l.index("Keller"))
        else:
            keller = None

        # Aufug
        if any(["Personenaufzug" in x for x in ausstattung_l]):
            aufzug = True
            ausstattung_l.pop(ausstattung_l.index("Personenaufzug"))
        else:
            aufzug = None

        # Gäste-WC
        if any(["Gäste-WC" in x for x in ausstattung_l]):
            gaeste_wc = True
            ausstattung_l.pop(ausstattung_l.index("Gäste-WC"))
        else:
            gaeste_wc = None

        # Garten/ -mitbenutzung
        if any(["Garten/ -mitbenutzung" in x for x in ausstattung_l]):
            garten = True
            ausstattung_l.pop(ausstattung_l.index("Garten/ -mitbenutzung"))
        else:
            garten = None

        # WG-geeignet
        if any(["WG-geeignet" in x for x in ausstattung_l]):
            wg_geeignet = True
            ausstattung_l.pop(ausstattung_l.index("WG-geeignet"))
        else:
            wg_geeignet = None

        # barrierefrei
        if any(["Stufenloser Zugang" in x for x in ausstattung_l]):
            barrierefrei = True
            ausstattung_l.pop(ausstattung_l.index("Stufenloser Zugang"))
        else:
            barrierefrei = None

        # Wohnberechtigungsschein wbs
        if any(["Wohnberechtigungsschein" in x for x in ausstattung_l]):
            wbs = True
            ausstattung_l.pop(
                ausstattung_l.index("Wohnberechtigungsschein erforderlich")
            )
        else:
            wbs = None

        assert len(ausstattung_l) == 0

    # keine Ausstattung angegeben
    else:
        online_besichtigung = None
        einbaukueche =  None
        balkon_terrasse =  None
        keller = None
        aufzug = None
        gaeste_wc = None
        garten = None
        wg_geeignet = None
        barrierefrei = None
        wbs = None

    ############################### Bausubstanz ###############################
    s = " Bausubstanz & Energieausweis "

    # Bausubstanz optional
    try:
        div = [
            x for x in soup.find_all("h4") if x.text == s
        ][0]
        bausubstanz_l = div.next.next.next
        bausubstanz_l = [x.text.strip() for x in bausubstanz_l.find_all("dl")]
        # Escape Sequences z.B. \x entfernen
        bausubstanz_l = [
            "".join([
                y for y in x if y.isascii() or y in ("äöüÄÖÜß")
            ]) for x in bausubstanz_l
        ]

        # replace double whitespace mit single white space
        bausubstanz_l = [x.replace("  ", " ") for x in bausubstanz_l]

        # Baujahr
        try:
            baujahr_geb_item = [
                x for x in bausubstanz_l if "Baujahr" in x
            ][0].split()[1]
            if "unbekannt" in baujahr_geb_item:
                baujahr_gebaeude = None
            else:
                baujahr_gebaeude = int(baujahr_geb_item)
        except IndexError:
            baujahr_gebaeude = None

        # Modernisierung
        try:
            modern_item = [
                x for x in bausubstanz_l if "Modernisierung" in x
            ][0]
            modernisierung_jahr = int(modern_item.split("zuletzt")[1])
        except IndexError:
            modernisierung_jahr = None

        # Zustand
        try:
            zustand_item = [
                x for x in bausubstanz_l if "Objektzustand" in x
            ][0].split("Objektzustand")[1].strip()
            zustand = zustand_d[zustand_item]
        except IndexError:
            zustand = 1

        try:
            # Ausstattung Qualitaet
            ausstattung_quali_item = [
                x for x in bausubstanz_l if "Ausstattung" in x
            ][0].split("Ausstattung")[1].strip()
            ausstattung_qualitaet = ausstattung_qualitaet_d[
                ausstattung_quali_item
            ]
        except IndexError:
            ausstattung_qualitaet = 1

        # Heizungsart
        try:
            heizungsart_item = [
                x for x in bausubstanz_l if "Heizungsart" in x
            ][0].split()[1]
            heizungsart = heizungsart_d[heizungsart_item]
        except IndexError:
            heizungsart = 1

        # Energieträger
        try:
            energietraeger_item = [
                x for x in bausubstanz_l if "Energieträger" in x
            ][0]
            energietraeger_item = " ".join(energietraeger_item.split()[2:])
            # nur das erste Imte wird als primärer Ebergieträger gezählt
            energietraeger_item = energietraeger_item.split(",")[0]
            energietraeger = energietraeger_d[energietraeger_item]
        except IndexError:
            energietraeger = 1

        # Energieausweis
        try:
            energieausweis_item = [
                x for x in bausubstanz_l if "Energieausweis " in x
            ][0]
            energieausweis = energieausweis_d[energieausweis_item]
        except IndexError:
            energieausweis = None

        # Energieausweistyp
        try:
            energieausweisart_item = [
                x for x in bausubstanz_l if "Energieausweistyp" in x
            ][0]
            energieausweis_art = energieausweisart_d[energieausweisart_item]
        except IndexError:
            energieausweis_art = None

        try:
            energieklasse_item = [
                x for x in bausubstanz_l if "Energieeffizienzklasse" in x
            ][0]
            energieeffizienzklasse = energieklasse_item.split(
                "Energieeffizienzklasse"
            )[1].strip()
        except IndexError:
            energieeffizienzklasse = None

    except IndexError:
        baujahr_gebaeude = None
        modernisierung_jahr = None
        zustand = 1
        ausstattung_qualitaet = 1
        heizungsart = 1
        energietraeger = 1
        energieausweis = None
        energieausweis_art = None
        energieeffizienzklasse = None

    ret_d = {
        "data_id": data_id,
        "titel": titel,
        "plz": plz,
        "adress_str": adress_str,
        "miete_kalt": miete_kalt,
        "nebenkosten": nebenkosten,
        "miete_heizkosten": miete_heizkosten,
        "miete_gesamt": miete_gesamt,
        "kosten_stellplatz": kosten_stellplatz,
        "kaution": kaution,
        "wohnungs_type": wohnungs_type,
        "etage": etage,
        "etage_all": etage_all,
        "wohnflaeche": wohnflaeche,
        "nutzflaeche": nutzflaeche,
        "frei_ab": frei_ab,
        "schlafzimmer": schlafzimmer,
        "badezimmer": badezimmer,
        "zimmer_anzahl": zimmer_anzahl,
        "haustiere": haustiere,
        "energietraeger": energietraeger,
        "garage_stellplatz": garage_stellplatz,
        "garage_stellplatz_cnt": garage_stellplatz_cnt,
        "schufa_auskunft": schufa_auskunft,
        "online_besichtigung": online_besichtigung,
        "einbaukueche": einbaukueche,
        "balkon_terrasse": balkon_terrasse,
        "keller": keller,
        "aufzug": aufzug,
        "gaeste_wc": gaeste_wc,
        "garten": garten,
        "wg_geeignet": wg_geeignet,
        "barrierefrei": barrierefrei,
        "wbs": wbs,
        "baujahr_gebaeude": baujahr_gebaeude,
        "modernisierung_jahr": modernisierung_jahr,
        "zustand": zustand,
        "ausstattung_qualitaet": ausstattung_qualitaet,
        "heizungsart": heizungsart,
        "energieausweis": energieausweis,
        "energieausweis_art": energieausweis_art,
        "energieeffizienzklasse": energieeffizienzklasse,
        "verfuegbar": True,
        "such_str": city
    }
    print(ret_d)
    print()

    return ret_d

# bisher gespeicherte ids holen
cur.execute(inserat_select_sql)
rows = cur.fetchall()
inserat_ids = [str(x[0]) for x in rows] if len(rows) > 0 else []
print(f"Bisher {len(inserat_ids)} inserate")

# page counter
count = 1
get_string = f"{url}Suche/de/bayern/{city}/{wtype_d[wtype]}?pagenumber="

# get max pagenumber
soup = http_get_to_soup(get_string + str(count))
select_wrapper = soup.find("div", class_="select-input-wrapper")
max_page_n = int(select_wrapper.find_all("option")[-1].text)

while count <= max_page_n:
    soup = http_get_to_soup(get_string + str(count))
    items_l = soup.find("ul", id="resultListItems")
    result_l = items_l.find_all("li", class_="result-list__listing")

    data_ids = [x.get("data-id") for x in result_l]

    # bereits geparste exposes filtern
    data_ids = [x for x in data_ids if x not in inserat_ids]
    if len(data_ids) == 0:
        print(f"{datetime.now()} keine neuen Inserate mehr für {city}")
        sys.exit(0)

    realt_items = [
        x.find("div",
               class_="result-list-entry__realtor-data") for x in result_l
    ]
    realtors = [
        " ".join([
            y.text for y in x.find_all("span") if y.text
        ]) for x in realt_items
    ]

    for data_id, realtor in zip(data_ids, realtors):
        print("Parsing expose: " + expo_url + data_id + "#/")
        soup = http_get_to_soup(expo_url + data_id + "#/")
        data = parse_expose(soup)
        data["realtor"] = realtor

        cur.execute(inserat_insert_sql, data)

        img_data = get_image(soup)
        cur.execute(images_insert_sql, (img_data, data_id))

    count += 1

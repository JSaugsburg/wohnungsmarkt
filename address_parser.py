import os
import time
import psycopg2
import requests
import configparser

from psycopg2.extras import Json

# path of script
script_path = os.path.dirname(os.path.realpath(__file__))

# main url wg-gesucht
url = "https://www.wg-gesucht.de/"

# these cities have no suburbs
no_suburb_l = [
    "Friedberg", "Dasing", "Königsbrunn", "Neusäß", "Aichach", "Dasing",
    "Stadtbergen", "Diedorf", "Affing", "Pöttmes", "Zusmarshausen",
    "Aystetten", "Gersthofen", "Friedberg", "Kutzenhausen", "Dinkelscherben",
    "Graben", "Großaitingen", "Igling", "Mering", "Kissing", "Welden",
    "Schwabmünchen", "Obergriesbach", "Obermeitingen"
]

select_inserate_sql = """
    SELECT inserat_id, viertel, city, adress_str
    FROM wg_gesucht.inserate
    WHERE osm_id IS NULL;
    """

osm_ids_sql = """
    SELECT osm_id FROM gis.osm;
    """

insert_osm_ids = """
    INSERT INTO gis.osm (osm_id, fc, city)
    VALUES (%s,%s,%s);
    """

update_city_sql = """
    UPDATE wg_gesucht.inserate
    SET city = %s,
    viertel = %s,
    strasse = %s,
    hausnummer = %s,
    plz = %s,
    neighbourhood = %s,
    lon = %s,
    lat = %s,
    osm_id = %s
    WHERE inserat_id = %s;
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

# get stored inserate
cur.execute(select_inserate_sql)
inserate = cur.fetchall()
print(f"Noch {len(inserate)} inserate")

# get stored osm_ids
cur.execute(osm_ids_sql)
rows = cur.fetchall()
osm_ids = [int(x[0]) for x in rows] if len(rows) > 0 else []
print(f"Bisher {len(osm_ids)} osm_ids")

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

def parse_address(address_str):
    query_str = "https://nominatim.openstreetmap.org/search?" \
        f"q={address_str}&format=geojson&addressdetails=1"
    print(query_str)
    r = requests.get(query_str)
    fc = r.json()
    feats = fc["features"]

    # bahnhof features rausfiltern
    feat = [
        x for x in feats if "station" not in x["properties"]["type"]
    ][0]

    # create FeatureCollection with only relevant feature
    fc["features"] = [feat]
    assert len(fc["features"]) == 1

    osm_id = feat["properties"]["osm_id"]
    if "city" in feat["properties"]["address"]:
        city = feat["properties"]["address"]["city"]
    elif "town" in feat["properties"]["address"]:
        city = feat["properties"]["address"]["town"]
    elif "village" in feat["properties"]["address"]:
        city = feat["properties"]["address"]["village"]

    if city in no_suburb_l:
        viertel = None
    else:
        viertel = feat["properties"]["address"]["suburb"] + "_" + city
        # map "Haunstetten" to "Haunstetten-Siebenbrunn"
        if viertel == "Haunstetten_Augsburg":
            viertel = "Haunstetten-Siebenbrunn_Augsburg"

    # bei folgenden Feature Types keine Strasse
    exclude_types = ("neighbourhood", "suburb", "administrative", "postcode")
    if feat["properties"]["type"] in exclude_types:
        strasse = None
    else:
        # verschiedene Abstufungen von "Strasse"
        if "road" in feat["properties"]["address"]:
            strasse = feat["properties"]["address"]["road"]
        elif "pedestrian" in feat["properties"]["address"]:
            strasse = feat["properties"]["address"]["pedestrian"]
        elif "footway" in feat["properties"]["address"]:
            strasse = feat["properties"]["address"]["footway"]
        elif "park" in feat["properties"]["address"]:
            strasse = feat["properties"]["address"]["park"]
        elif "leisure" in feat["properties"]["address"]:
            strasse = feat["properties"]["address"]["leisure"]

    if "house_number" in feat["properties"]["address"]:
        hausnummer = feat["properties"]["address"]["house_number"]
    else:
        hausnummer = None

    # manche features weisen keine plz auf; hier überprüfen
    if "postcode" in feat["properties"]["address"]:
        plz = feat["properties"]["address"]["postcode"]
    else:
        plz = None

    if "neighbourhood" in feat["properties"]["address"]:
        neighbourhood = feat["properties"]["address"]["neighbourhood"]
    else:
        neighbourhood = None

    # get coords
    assert feat["geometry"]["type"] == "Point"
    lon = feat["geometry"]["coordinates"][0]
    lat = feat["geometry"]["coordinates"][1]

    # insert osm if necessary
    if osm_id not in osm_ids:
        cur.execute(insert_osm_ids, [osm_id, Json(fc), city])
        # add new osm_id to list
        osm_ids.append(osm_id)

    ret_d = {
        "osm_id": osm_id,
        "viertel": viertel,
        "city": city,
        "strasse": strasse,
        "hausnummer": hausnummer,
        "plz": plz,
        "neighbourhood": neighbourhood,
        "lon": lon,
        "lat": lat
    }

    return ret_d


for i in inserate:
    if len(i[3].split()) == 2:
        address_str = i[2] + " " + i[3].split()[1]
    else:
        address_str = i[2] + " " + " ".join(i[3].split()[2:])
    address_str = address_str.replace(" ", "+")
    print(i[0])
    print(i[3] + " .... " + address_str)
    osm_data = parse_address(address_str)
    print(osm_data)
    if i[2] != osm_data["city"]:
        print(f"---replacing {osm_data['city']} for {i[2]}----")
    if i[1] != osm_data["viertel"]:
        print(f"---replacing {osm_data['viertel']} for {i[1]}----")

    cur.execute(update_city_sql, (
        osm_data["city"],
        osm_data["viertel"],
        osm_data["strasse"],
        osm_data["hausnummer"],
        osm_data["plz"],
        osm_data["neighbourhood"],
        osm_data["lon"],
        osm_data["lat"],
        osm_data["osm_id"],
        i[0],
    ))
    time.sleep(2)
    print()

cur.close()
conn.close()

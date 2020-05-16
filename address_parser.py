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
    UPDATE wg_gesucht.dup_inserate
    SET city = %s,
    viertel = %s,
    strasse = %s,
    hausnummer = %s,
    plz = %s,
    neighbourhood = %s,
    lon = %s,
    lat = %s
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
    city = feat["properties"]["address"]["city"]
    viertel = feat["properties"]["address"]["suburb"] + "_" + city
    strasse = feat["properties"]["address"]["road"]

    if "house_number" in feat["properties"]["address"]:
        hausnummer = feat["properties"]["address"]["house_number"]
    else:
        hausnummer = None

    plz = feat["properties"]["address"]["postcode"]

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
    address_str = i[2] + " " + " ".join(i[3].split()[2:])
    address_str = address_str.replace(" ", "+")
    print(i[0])
    print(address_str)
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
        i[0],
    ))
    time.sleep(2)
    print()

cur.close()
conn.close()

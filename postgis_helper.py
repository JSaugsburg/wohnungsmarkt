import os
import psycopg2
import geopandas as gpd

conn = psycopg2.connect("dbname=wohnungsmarkt_db user=sepp")
cur = conn.cursor()

f_path = os.getcwd() + "/geojson/augsburg/"
aux_viertel = gpd.read_file(f_path + "augsburg_viertel_all.geojson")


for v in aux_viertel.iterrows():
    viertel = v[1].display_name.split(", ")[0]

    cur.execute(
        "INSERT INTO gis.viertel(geom, name, city, viertel_city)"
        "VALUES (ST_SetSRID(%(geom)s::geometry, %(srid)s), %(name)s, %(city)s, %(viertel_city)s)",
        {
            "geom": v[1].geometry.wkb_hex,
            "srid": 4326,
            "name": viertel,
            "city": "Augsburg",
            "viertel_city": viertel + "_Augsburg"
        }
    )
conn.commit()
cur.close()
conn.close()

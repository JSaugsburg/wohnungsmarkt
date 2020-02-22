import csv
import os
import geojson as gj
import psycopg2
from shapely.geometry import shape

conn = psycopg2.connect("dbname=wohnungsmarkt_db user=sepp")
cur = conn.cursor()

with open("zuordnung_plz_ort.csv") as csv_f:
    plz_reader = csv.reader(csv_f)

plz_aux = plz_reader
print(plz_aux)

f_path = os.getcwd() + "/geojson/augsburg/"
with open(f_path + "augsburg_viertel_all.geojson") as f:
    fc = gj.load(f)

for feat in fc["features"]:
    poly_wkt = shape(feat["geometry"]).wkt
    cur.execute("""
        INSERT INTO gis.viertel()""")


    print(shp.wkt)


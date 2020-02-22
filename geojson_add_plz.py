import csv
import os
import geojson as gj
import psycopg2
from shapely.geometry import shape

conn = psycopg2.connect("dbname=wohnungsmarkt_db user=sepp")
cur = conn.cursor()

with open("zuordnung_plz_ort.csv") as csv_f:
    plz_reader = csv.reader(csv_f)
    plz_aux = [x for x in plz_reader if x[1] == "Augsburg"]
print(plz_aux)

f_path = os.getcwd() + "/geojson/augsburg/"
with open(f_path + "augsburg_viertel_all.geojson") as f:
    fc = gj.load(f)

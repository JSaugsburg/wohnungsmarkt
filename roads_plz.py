import os
import geojson as gj
import geopandas as gpd

f_path = os.getcwd() + "/geojson/augsburg/"

plz_shp = gpd.read_file("plz-gebiete.shp")
aux_shp = plz_shp[plz_shp["note"].str.contains("Augsburg")]
roads_gdf = gpd.read_file(f_path + "/augsburg_roads.geojson")
with open(f_path + "/augsburg_roads.geojson") as f:
    roads_gj = gj.load(f)

osm_ids = roads_gdf["osm_id"]
for osm_id in osm_ids:
    sub_gdf = roads_gdf[roads_gdf["osm_id"] == osm_id]
    sjoin = gpd.sjoin(aux_shp, sub_gdf, how="inner", op="intersects")
    if len(sjoin) != 1:
        print(sjoin)
        break
    #plz = sjoin["plz"]
    """
    # find feature with "osm_id"
    feat = [
        x for x in roads_gj["features"] if x["properties"]["osm_id"] == osm_id
    ]
    # add viertel to properties
    feat[0]["properties"]["viertel"] = viertel

with open(f_path + "/augsburg_roads.geojson", "w") as outfile:
    gj.dump(roads_gj, outfile)
    """

import os
import geojson as gj

f_path = os.getcwd() + "/geojson/augsburg/viertel/"

fc_list = []
files = os.listdir(f_path)
for file in files:
    with open(f_path + file) as f:
        j = gj.load(f)
        #poly = gj.Polygon(
        # FeatureCollection mit nur einem Feature
        if len(j["features"]) == 1:
            # Polygon without hole
            coords = j["features"][0]["geometry"]["coordinates"]
            poly = gj.Polygon(coords)
            props = j["features"][0]["properties"]
            feat = gj.Feature(geometry=poly, properties=props)
            fc_list.append(feat)
        else:
            print(file + "'s Laenge passt nicht")

feature_collection = gj.FeatureCollection(fc_list)
print(feature_collection)
print(feature_collection.is_valid)
out_str = os.getcwd() + "/geojson/augsburg/augsburg_viertel_all.geojson"
with open(out_str, "w") as out:
    gj.dump(feature_collection, out)


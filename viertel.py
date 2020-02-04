import requests

nominatim_str = "https://nominatim.openstreetmap.org/search?q="
land = "Bayern"
city = "Augsburg"
viertel_ls = [
    "Innenstadt", "Oberhausen", "Bärenkeller", "Firnhaberau", "Hammerschmiede",
    "Lechhausen", "Kriegshaber", "Pfersee", "Hochfeld", "Antonsviertel",
    "Spickel-Herrenbach", "Hochzoll", "Haunstetten-Siebenbrunn", "Göggingen",
    "Inningen", "Bergheim", "Universitätsviertel"
]

for v in viertel_ls:
    #r = requests.get(
    #    nominatim_str + v + "%2c+" + city + "%2c+" + land + "&format=json"
    #)
    print(nominatim_str + v + "%2c+" + city + "%2c+" + land + "&format=json")
    print(nominatim_str + v + "%2c+" + city + "%2c+" + land + "&format=json")
    #print(r.json())
    #break

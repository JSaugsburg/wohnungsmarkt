import requests

nominatim_str = "https://nominatim.openstreetmap.org/search?q="
city = "Augsburg"
viertel_ls = [
    "Innenstadt", "Oberhausen", "Bärenkeller", "Firnhaberau", "Hammerschmiede",
    "Lechhausen", "Kriegshaber", "Pfersee", "Hochfeld", "Antonsviertel",
    "Spickel-Herrenbach", "Hochzoll", "Haunstetten-Siebenbrunn", "Göggingen",
    "Inningen", "Bergheim", "Universitätsviertel"
]

for v in viertel_ls:
    r = requests.get(
        nominatim_str + "17+Strada+Pictor+Alexandru+Romano%2C+Bukarest&format=json")

print(r)

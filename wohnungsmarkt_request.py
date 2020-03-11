from datetime import datetime
from wohnungsmarkt import WgGesucht
import os
import random
import sys
import time

print(datetime.now())
wg = WgGesucht(0, "Augsburg")
p_cnt = wg.get_page_counter()
with open(os.path.dirname(os.path.realpath(__file__)) + "/wg_counter") as f:
    wg_counter = f.read()
print("wg_counter: " + wg_counter)
for i in range(int(wg_counter), p_cnt):
    urls_a = wg.get_urls(i)
    urls = [
        x for x in urls_a if wg.get_id_of_url(x) not in wg.inserat_ids
    ]
    for url in urls:
        print(url)
        parsed_wg = wg.parse_wgs(url)
        if parsed_wg == 1:
            print("Captcha appeared!")
            sys.exit(1)
        wg.insert_into_inserate(parsed_wg)
        time.sleep(random.choice([4, 5, 6, 7, 8]))
    with open(os.path.dirname(os.path.realpath(__file__)) + "/wg_counter", "w") as f:
        f.write(str(i))
    print("wg_counter jetzt bei " + str(i))

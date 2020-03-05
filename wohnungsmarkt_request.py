from wohnungsmarkt import WgGesucht
import random
import time

wg = WgGesucht(1, "Augsburg")
p_cnt = wg.get_page_counter()
for i in range(p_cnt):
    urls = wg.get_urls(i)
    for url in urls:
        print(url)
        parsed_wg = wg.parse_wgs(url)
        if parsed_wg["inserat_id"] not in wg.inserat_ids:
            wg.insert_into_inserate(parsed_wg)
            time.sleep(random.choice([4, 5, 6, 7, 8]))

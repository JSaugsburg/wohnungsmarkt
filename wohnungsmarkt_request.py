from wohnungsmarkt import WgGesucht
import random
import time

wg = WgGesucht(1, "Augsburg")
p_cnt = wg.get_page_counter()
for i in range(p_cnt):
    urls_a = wg.get_urls(i)
    urls = [x for x in urls_a if wg.get_id_of_url(x) not in wg.inserat_ids]
    for url in urls:
        parsed_wg = wg.parse_wgs(url)
        wg.insert_into_inserate(parsed_wg)
        time.sleep(random.choice([4, 5, 6, 7, 8]))

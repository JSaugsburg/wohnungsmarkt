from wohnungsmarkt import WgGesucht

wg = WgGesucht(1, "Augsburg")
p_cnt = wg.get_page_counter()
for i in range(p_cnt):
    urls = wg.get_urls(i)
    for url in urls:
        parsed_wg = wg.parse_wgs(url)
        wg.insert_into_inserate(parsed_wg)
        break
    break
wg.conn.commit()

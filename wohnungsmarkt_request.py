from wohnungsmarkt import WgGesucht

wg = WgGesucht(1, "Augsburg")
print(wg.inserat_sql)
p_counter = wg.get_page_counter()
"""
for i in range(p_cnt):
    urls = wg.get_urls(i)
    for url in urls:

        #parsed = wg.parse_

"""

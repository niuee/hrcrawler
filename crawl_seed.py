from crawler import ReqBuilder
from bs4 import BeautifulSoup



active_url = "https://db.netkeiba.com/?pid=horse_list&act=1&sort_key=prize&sort_type=desc&page=" 
req_builder = ReqBuilder()
seed_list = []

def crawl_ranking(start_page, end_page):
    ranking_url = "https://db.netkeiba.com/?pid=ranking_list&hr=ninki&sort=daily&page="
    for page in range(start_page, end_page + 1):
        res = req_builder.get_page(ranking_url + str(page))
        soup = BeautifulSoup(res, "html.parser")

        ptags = soup.findAll('p', attrs={'class': 'rank_horse'})
        for ptag in ptags:
            # print(ptag)
            horse_link = ptag.find('a')['href']
            # print(horse_link)
            id = horse_link.split('/')[-2]
            seed_list.append(id)

def crawl_active(start_page, end_page):
    for page in range(start_page, end_page + 1):
        res = req_builder.get_page(active_url + str(page))
        soup = BeautifulSoup(res, "html.parser")
        table = soup.find('table', attrs={'summary': '競走馬検索結果'})
        table_rows = table.find_all('tr')
        for row in table_rows:
            tds = row.findAll('td')
            if tds:
                seed_list.append(tds[1].find('a')['href'].split('/')[-2])

crawl_active(110, 130)
f = open('crawl_seed_list.txt', 'w+')
for id in seed_list:
    f.write(id + '\n')
f.close()
import requests
import urllib.parse
import time
import sys
import signal
import os
from collections import deque
from bs4 import BeautifulSoup
from dbconnect import DBCon

ranges = [
  {"from": ord(u"\u3300"), "to": ord(u"\u33ff")},         # compatibility ideographs
  {"from": ord(u"\ufe30"), "to": ord(u"\ufe4f")},         # compatibility ideographs
  {"from": ord(u"\uf900"), "to": ord(u"\ufaff")},         # compatibility ideographs
  {"from": ord(u"\U0002F800"), "to": ord(u"\U0002fa1f")}, # compatibility ideographs
  {'from': ord(u'\u3040'), 'to': ord(u'\u309f')},         # Japanese Hiragana
  {"from": ord(u"\u30a0"), "to": ord(u"\u30ff")},         # Japanese Katakana
  {"from": ord(u"\u2e80"), "to": ord(u"\u2eff")},         # cjk radicals supplement
  {"from": ord(u"\u4e00"), "to": ord(u"\u9fff")},
  {"from": ord(u"\u3400"), "to": ord(u"\u4dbf")},
  {"from": ord(u"\U00020000"), "to": ord(u"\U0002a6df")},
  {"from": ord(u"\U0002a700"), "to": ord(u"\U0002b73f")},
  {"from": ord(u"\U0002b740"), "to": ord(u"\U0002b81f")},
  {"from": ord(u"\U0002b820"), "to": ord(u"\U0002ceaf")}  # included as of Unicode 8.0
]

def is_cjk(char):
  return any([range["from"] <= ord(char) <= range["to"] for range in ranges])

class ReqBuilder:

    def __init__(self) -> None:
        self.last_request_time = time.time()
    
    def get_page(self, url):
        if time.time() - self.last_request_time < 1:
            time.sleep(1)
        self.last_request_time = time.time()
        res = requests.get(url)
        if res.status_code != 200:
            return None
        res.encoding = 'euc-jp'
        return res.text

class HorseCrawler:

    def __init__(self) -> None:
        self.start_time = time.time()
        self.crawl_list = deque()
        self.req_builder = ReqBuilder()
        self.crawl_count = 0
        self.crawl_limit = 100
        self.db_con = DBCon()
        self.born_place_map = {"加": "CANADA", "米": "USA", "洪": "HUNGARY", "英": "ENGLAND", "伊": "ITALY", "仏": "FRANCE", "愛": "IRELAND", "新": "NEWZEALAND", "独": "GERMANY", "豪": "AUSTRALIA"}
    
    def __del__(self):
        self.db_con.disconnect()
    
    def graceful_shutdown(self, signum, frame):
        print("[LOG] shutting down...")

        f = open("crawl_continue_list.txt", "w+")
        for horse_id in self.crawl_list:
            if horse_id:
                f.write(horse_id + "\n")
        f.close()
        self.db_con.disconnect()
        end_time = time.time()
        print("time: ", end_time - start_time)
        print("crawl count: ", self.crawl_count)
        print("crawling ended in grace shutdown")
        sys.exit(0)
    
    def set_crawl_list(self, horse_id_list):
        self.crawl_list = deque(horse_id_list)
        self.crawl_count = 0
    
    def add_crawl_list(self, horse_id):
        self.crawl_list.append(horse_id)
    
    def limited_crawl(self):
        while self.crawl_list and self.crawl_count < self.crawl_limit:
            horse_id = self.crawl_list.popleft()
            print("---------------------------------------")
            print("[LOG] going into crawling of:", horse_id)
            try:
                if self.crawl_horse(horse_id, recusive=False):
                    self.crawl_count += 1
            except Exception as e:
                print("[ERROR]:", e)
        self.db_con.disconnect()
    
    def crawl(self):
        self.start_time = time.time()
        while self.crawl_list and self.crawl_count < self.crawl_limit:
            horse_id = self.crawl_list.popleft()
            print("---------------------------------------")
            print("[LOG] current crawl count:", self.crawl_count)
            print("[LOG] crawl percentage:", str(self.crawl_count / self.crawl_limit * 100) + "%")
            print("[LOG] going into crawling of:", horse_id)
            try:
                if self.crawl_horse(horse_id, recusive=True):
                    self.crawl_count += 1
            except Exception as e:
                print("[ERROR]:", e)
        
        if len(self.crawl_list) == 0:
            print("[LOG] crawling ended in normal shutdown (exhausted crawl list)")

        self.db_con.disconnect()
        f = open("crawl_continue_list.txt", "w+")
        for horse_id in self.crawl_list:
            if horse_id:
                f.write(horse_id + "\n")
        f.close()
    
    def get_direct_text_only(self, parent):
        return ''.join(parent.find_all(string=True, recursive=False)).strip()
    
    def crawl_horse(self, horse_id: str, recusive: bool) -> bool:
        print("[LOG] crawling horse id:", horse_id)
        if horse_id == "" or self.db_con.horse_populated(horse_id):
            print("[LOG] dupilcate horse")
            return False
        res = self.req_builder.get_page('https://db.netkeiba.com/horse/' + horse_id + '/')
        if res == None:
            return False

        soup = BeautifulSoup(res, 'html.parser')

        horse_title = soup.find('div', attrs={'class': 'horse_title'})
        profile_table = soup.find('table', attrs={'summary': 'のプロフィール'})
        year = None
        sex = None
        sire_id = None
        dam_id = None
        jra_registered = None
        fur_color = None
        alt_name = None
        classic = None
        born_place = None
        actively_racing = None

        # get year
        if profile_table and profile_table.find('td').text:
            born_location_row_data = profile_table.findAll('td')[4].text
            if born_location_row_data == "":
                print("[LOG]: no born location provided")
            else:
                if born_location_row_data in self.born_place_map:
                    born_place = self.born_place_map[born_location_row_data]
                else:
                    born_place = born_location_row_data
            born_date_row_data = profile_table.find('td').text
            year_char = born_date_row_data.index('年')
            if year_char != -1:
                year = born_date_row_data[:year_char]
            else:
                print("[LOG]: no born date provided")
        
        # get name
        name = soup.find('div', attrs={'class': 'horse_title'}).find('h1').text
        if name == "":
            return False

        # get attrs
        attrs = soup.find('div', attrs={'class': 'horse_title'}).find('p', attrs={'class': 'txt_01'})
        attrs = next(attrs.strings)
        attrs = attrs.replace(" ", "").split("\u3000")

        # get horse sex
        if len(attrs) > 2:
            sex = attrs[1][0]

        # get fur color
        if len(attrs) > 3:
            fur_color = attrs[2]
        if fur_color and fur_color == "":
            print("[LOG]: no fur color provided")
            fur_color = None

        if attrs: 
            if attrs[0] == "抹消" or attrs[0] == "現役":
                if attrs[0] == "現役":
                    actively_racing = True
                    classic = False
                elif attrs[0] == "抹消":
                    actively_racing = False
                jra_registered = True

        # get classic status
        classic_tag = soup.find('div', attrs={'class': 'horse_title'}).find('p', attrs={'class': 'txt_01'}).find('span')
        if classic_tag:
            classic = True
            jra_registered = True

        # get alt name
        eng_para_tag = horse_title.find('p', attrs={'class': 'eng_name'})
        if eng_para_tag:
            alt_name = horse_title.find('a').text
            # print("Alt name raw:", alt_name)
            if is_cjk(alt_name[0]):
                print("[LOG]: alt name is not in alphabets")
            else:
                print("[LOG]: alt name is in alphabets")
                name, alt_name = alt_name, name
        else:
            print("[LOG]: no alt name provided")
        
        pedigree_table = soup.find('table', attrs={'class': 'blood_table'}).findAll('tr')

        sire = pedigree_table[0].find('td').find('a')
        if sire.text == "":
            print("[LOG]: no sire provided")
            sire = None
        else:
            sire_id = sire['href'].split("/")[3]
            sire = sire.text

        dam = pedigree_table[2].find('td').find('a')
        if dam.text == "":
            print("[LOG]: no dam provided")
            dam = None
        else:
            dam_id = dam['href'].split("/")[3]
            dam = dam.text
        
        if sex:
            if sex == '牡':
                sex = 'Horse'
            elif sex == '牝':
                sex = 'Mare'
            elif sex == 'セ':
                sex = 'Gelding'

        print("name:", name)
        print("alt name:", alt_name)
        print("sex:", sex)
        print("born year:", year)
        print("fur color:", fur_color)
        print("sire: {}, sire id: {}".format(sire, sire_id))
        print("dam: {}, dam id: {}".format(dam, dam_id))
        print("registered or once registered") if jra_registered else print("registration undetermined")



        if sire_id and not self.db_con.horse_exists(sire_id):
            self.db_con.insert_horse_with_placeholder(sire_id)
        
        if dam_id and not self.db_con.horse_exists(dam_id):
            self.db_con.insert_horse_with_placeholder(dam_id)


        if not self.db_con.horse_exists(horse_id):
            self.db_con.insert_horse_with_placeholder(horse_id)

        try:
            self.db_con.insert_horse_attribute(horse_id, "horse_name", name)
            if alt_name:
                self.db_con.insert_horse_attribute(horse_id, "alt_name", alt_name)
            if year:
                self.db_con.insert_horse_attribute(horse_id, "born_date", year)
            if sex:
                self.db_con.insert_horse_attribute(horse_id, "horse_sex", sex)
            if sire_id:
                self.db_con.insert_horse_attribute(horse_id, "sire_id", sire_id)
            if dam_id:
                self.db_con.insert_horse_attribute(horse_id, "dam_id", dam_id)
            if fur_color:
                self.db_con.insert_horse_attribute(horse_id, "fur_color", fur_color)
            if jra_registered:
                self.db_con.insert_horse_attribute(horse_id, "jra_registered", jra_registered)
            if classic:
                self.db_con.insert_horse_attribute(horse_id, "classic", classic)
            if actively_racing:
                self.db_con.insert_horse_attribute(horse_id, "active", actively_racing)
            if born_place:
                self.db_con.insert_horse_attribute(horse_id, "born_place", born_place)
            self.db_con.commit_change()
        except Exception as e:
            print("[LOG]: failed to insert horse attribute")
            print("[ERROR]:", e)
            self.db_con.rollback_change()
            return False
        
        if recusive:
            if sex == "Horse":
                name_in_param = urllib.parse.quote(name.encode('euc-jp'))
                offspring_search_url = "https://db.netkeiba.com/?pid=horse_list&sire=" + name_in_param
                test_res = self.req_builder.get_page(offspring_search_url)
                if test_res != None:
                    test_soup = BeautifulSoup(test_res, 'html.parser')
                    last_page_url = test_soup.find('a', attrs={'title': '最後'})
                    last_page_number = 1
                    if last_page_url != None:
                        last_page_url = last_page_url['href']
                        last_page_number = int(urllib.parse.parse_qs(last_page_url)['page'][0])
                    last_page_number = min(last_page_number, 3)
                    for page in range(1, last_page_number):
                        offspring_search_url = "https://db.netkeiba.com/?pid=horse_list&sire=" + name_in_param + "&page=" + str(page) + "&sort_key=prize&sort_type=desc"
                        res = self.req_builder.get_page(offspring_search_url)
                        if res != None:
                            soup = BeautifulSoup(res, 'html.parser')
                            table = soup.find('table', attrs={'summary': '競走馬検索結果'})
                            for tr in table.findAll('tr'):
                                row_data = tr.findAll('td')
                                if row_data:
                                    child_horse_id = tr.findAll('td')[1].find('a')['href'].split("/")[2]
                                    if child_horse_id != "":
                                        if self.db_con.horse_populated(child_horse_id):
                                            continue
                                        self.add_crawl_list(child_horse_id)
            elif sex == "Mare":
                offspring_search_url = "https://db.netkeiba.com/mare/" + horse_id + "/"
                res = self.req_builder.get_page(offspring_search_url)
                if res != None:
                    soup = BeautifulSoup(res, 'html.parser')
                    table = soup.find('table', attrs={'summary': '成績'})
                    for tr in table.findAll('tr'):
                        row_data = tr.findAll('td')
                        if row_data:
                            child_horse_id = tr.findAll('td')[1].find('a')['href'].split("/")[2]
                            if child_horse_id != "":
                                if self.db_con.horse_populated(child_horse_id):
                                    continue
                                self.add_crawl_list(child_horse_id)
            if not self.db_con.horse_populated(sire_id):
                self.add_crawl_list(sire_id)
            if not self.db_con.horse_populated(dam_id):
                self.add_crawl_list(dam_id)
        return True
if __name__ == "__main__":
    crawl_list_dir = "./crawl lists"
    f = open(os.path.join(crawl_list_dir, "crawl_seed_list.txt"), 'r')
    lines = f.read().splitlines()
    f.close()

    start_time = time.time()
    print("start crawling")
    test_crawler = HorseCrawler()
    test_crawler.crawl_limit = 20000
    signal.signal(signal.SIGINT, test_crawler.graceful_shutdown)
    test_crawler.set_crawl_list(lines)
    test_crawler.crawl()
    end_time = time.time()
    print("time: ", end_time - start_time)
    print("crawl count: ", test_crawler.crawl_count)
    print("crawling ended")
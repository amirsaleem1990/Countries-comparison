import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import pickle

url = "http://www.mobile-phone.pk/mobile_brands/"

def get_brands_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    brands_links = []
    a = soup.find_all("div", {"class" : "block_wrapper"})[1].find_all("div", {"class" : "table_cell"})
    for i in a:
        brands_links.append(i.find("a")['href'])
    with open("brands_links.pkl", "wb") as file:
        pickle.dump(brands_links, file)
get_brands_links()

def get_all_links():
    all_links = []
    with open("brands_links.pkl", "rb") as file:
        brands_links = pickle.load(file)

    for brand_url in brands_links:
        try:
            soup = BeautifulSoup(requests.get(brand_url,timeout=5).text, "lxml")
            one_brand_links = [brand_url]
            b = soup.find("span", {"style" : "float: left; margin-left: 5px; line-height: 22px;width: 100%;"}).                                                                    find_all("a")[-1]['href']
            pages = int(re.findall('\d+',b)[0])
            for i in range(1, pages+1):
                one_brand_links.append("{}-{}/".format(brand_url, i))
            all_links += one_brand_links
        except:
            all_links.append(brand_url)
    with open("all_links.pkl", "wb") as file:
        pickle.dump(all_links, file)
get_all_links()

def links_to_data():
    with open("all_links.pkl", "rb") as file:
        all_links = pickle.load(file)

    mobiles = []
    prices = []
    currency = []
    errors = []
    ee = 0
    for link in all_links:
        ee += 1
        print(ee, end=", ")
        try:
            soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
            a = soup.find("div", {"class" : "center_mobs"}).find_all("div", {"class" : "home_page_blocks"})
            m1 = []
            cc1 = []
            p1 = []
            for i in a:
                b = i.get_text().strip()
                m = b[:b.rfind("\n")].strip()
                c = b[b.find("\n"):].strip()
                if not "Coming" in c:
                    cc = c[:c.find(" ")].replace(".", "").strip()
                    p = c[c.find(" "):].strip()
                else:
                    cc = "Coming Soon"
                    p = "Coming Soon"
                m1.append(m)
                cc1.append(cc)
                p1.append(p)
            mobiles += m1
            currency += cc1
            prices += p1
        except:
            errors.append(link)
            pass
    if errors:
        print("There is {} errors, those links with error saved in file *errors.pkl*".format(len(errors)))
        with open("errors3.pkl", "wb") as file: 
            pickle.dump(errors, file)
    with open("currency.pkl", "wb") as file: 
        pickle.dump(currency, file)
    with open("prices.pkl", "wb") as file: 
        pickle.dump(prices, file)
    with open("mobiles.pkl", "wb") as file: 
        pickle.dump(mobiles, file)
links_to_data()

def data_to_csv():
    with open("currency.pkl", "rb") as file: 
        currency = pickle.load(file)
    with open("prices.pkl", "rb") as file: 
        prices = pickle.load(file)
    with open("mobiles.pkl", "rb") as file: 
        mobiles = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency', 'new_used']:
        df[i] = eval(i)
    df['web'] = [url]*len(mobiles)
    df.to_csv("data-mobile-phone-pak-new.csv", index=False)
data_to_csv()


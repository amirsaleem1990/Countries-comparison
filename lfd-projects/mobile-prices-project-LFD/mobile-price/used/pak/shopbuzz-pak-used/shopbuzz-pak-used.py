import pandas as pd
import pickle
import re
import os
from bs4 import BeautifulSoup
import requests

url = "http://www.shopbuzz.pk/usedproduct/index?category=mobile-phones"

def get_all_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    a = soup.find("div", {"id" : "yw0"}).find("ul").find_all("li")[-1].find("a")['href']
    pages = int(re.findall('\\d+',a)[0])
    all_links = [url]
    for i in range(2, pages+1):
        all_links.append("http://www.shopbuzz.pk/usedproduct/index?category=mobile-phones&UsedProduct_page=" + str(i))
    with open("all_links.pkl", "wb") as file:
    pickle.dump(all_links, file)
get_all_links()

def data_from_links():
    with open("all_links.pkl", "rb") as file:
        all_links = pickle.load(file)
    mobiles = []
    prices = []
    currency = []
    for link in all_links:
        try:
            soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
            a = soup.find("table", {"class" : "items table table-bordered"}).find("tbody")
            b = a.find_all("tr", {"class" : "odd"})
            b += a.find_all("tr", {"class" : "even"})
            m1 = []
            cc1 = []
            pp1 = []
            for i in b:
                d = i.find_all("td")
                m = d[0].get_text().strip()
                c = d[-1].get_text().strip()
                cc = c.split()[0].replace(".", "").strip()
                p = c.split()[1].replace(".", "").strip()
                pp = int(''.join(re.findall('\\d+',p)))

                m1.append(m)
                cc1.append(cc)
                pp1.append(pp)
            mobiles += m1
            currency += cc1
            prices += pp1
        except:
            pass
    with open("mobiles.pkl", "wb") as file: 
    	pickle.dump(mobiles, file)
    with open("currency.pkl", "wb") as file: 
    	pickle.dump(currency, file)
    with open("prices.pkl", "wb") as file: 
    	pickle.dump(prices, file)
data_from_links()

def data_to_csv():
    with open("mobiles.pkl", "rb") as file: 
        mobiles = pickle.load( file)
    with open("currency.pkl", "rb") as file: 
        currency = pickle.load( file)
    with open("prices.pkl", "rb") as file: 
        prices = pickle.load( file)
    web = [url]*len(mobiles)
    df = pd.DataFrame()
    new_used = ['used']*len(mobiles)
    for i in ['mobiles','prices','currency','new_used', 'web']:
        df[i] = eval(i)
    df.to_csv("data-shopbuzz-pak-used.csv", index=False)
data_to_csv()
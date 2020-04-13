import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import pickle

url = "https://eg.pricena.com/en/mobile-tablets/mobile-phones"

def get_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    all_links = ["https://eg.pricena.com/en/mobile-tablets/mobile-phones"]
    c = 1
    while True:
        c += 1
        ur = "https://eg.pricena.com/en/mobile-tablets/mobile-phones/page/" + str(c)
        u = requests.get(ur,timeout=5).history
        if not u:
            all_links.append(ur)
        else:
            break
    with open("links.pkl", "wb") as file:
        pickle.dump(all_links,file)
get_links()

def links_to_data():
    with open("links.pkl", "rb") as file:
        links = pickle.load(file)
    mobiles = []
    prices = []
    currency =  []
    for link in links:
        try:
            soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
            a = soup.find("div", {"id" : "results"})
            b = a.find_all("div", {"class" : "item desktop"})
            for c in b:
                m = c.find("div", {"class" : "name leftdirection"}).get_text().strip()
                p = c.find("div", {"class" : "price"}).get_text().strip()
                mobiles.append(m)
                prices.append(p.split()[1])
                currency.append(p.split()[0])
        except: 
            pass
    web = [url]*len(mobiles)
    with open("mobiles.pkl", "wb") as file: 
        pickle.dump(mobiles, file)
    with open("prices.pkl", "wb") as file: 
        pickle.dump(prices, file)
    with open("currency.pkl", "wb") as file: 
        pickle.dump(currency, file)
    with open("web.pkl", "wb") as file: 
        pickle.dump(web, file)
links_to_data()

def data_to_csv():
    with open("mobiles.pkl", "rb") as file:     
        mobiles = pickle.load(file)
    with open("prices.pkl", "rb") as file:      
        prices = pickle.load(file)
    with open("currency.pkl", "rb") as file:   
        currency = pickle.load(file)
    with open("web.pkl", "rb") as file:   
        web = pickle.load(file)
    df = pd.DataFrame()
    new_used = ['new']*len(mobiles)
    for i in ['mobiles', 'prices', 'currency','new_used', 'web']:
        df[i] = eval(i)
    df.to_csv("pricena-egypt-new.csv", index=False)
data_to_csv()
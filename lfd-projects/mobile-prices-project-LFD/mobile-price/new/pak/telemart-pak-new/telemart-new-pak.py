import requests
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
import pickle

url = "https://www.telemart.pk/mobile-and-tablets/mobile-phone.html"
url += "?limit=all" # all result in one page
def links_to_data():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    prices = []
    mobiles = []
    currency = []
    a = soup.find("div", {"id" : "catalog-listing"})
    b = a.find_all("li", {"class" : "col-lg-3 col-md-3 col-sm-5 col-xs-12 item"})
    for mobile in b:
        c = mobile.find("div", {"class" : "pro-inner"})
        nam = c.find("div", {"class" : "pro-title product-name"}).get_text().strip()
        d = c.find("div", {"class" : "pro-content"}).get_text().strip()
        cur = d.split()[0].strip()
        prc = d.split()[-1].strip().replace(",", "")
        prices.append(prc)
        currency.append(cur)
        mobiles.append(nam)
    with open("prices.pkl", "wb") as file: 
        pickle.dump(prices, file)
    with open("currency.pkl", "wb") as file: 
        pickle.dump(currency, file)
    with open("mobiles.pkl", "wb") as file: 
        pickle.dump(mobiles, file)
links_to_data()

def data_to_csv():
    with open("prices.pkl", "rb") as file:
        prices = pickle.load(file)
    with open("currency.pkl", "rb") as file:
        currency = pickle.load(file)
    with open("mobiles.pkl", "rb") as file:
        mobiles = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency', 'new_used']:
        df[i] = eval(i)
    df['web'] = [url]*len(mobiles)
    df.to_csv("data-telemart-pak-new.csv", index=False)
data_to_csv()
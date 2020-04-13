import requests
from bs4 import BeautifulSoup
import pickle
import pandas as pd
import os

url = "https://egypt.souq.com/eg-en/mobile-phone/l/?section=2&page=1"

def all_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    mobiles_qty = int(''.join([i for i in soup.find("li", {"class" : "total"}).get_text() if i.isnumeric()]))
    if mobiles_qty % 60 == 0:
        pages = mobiles_qty // 60
    else:
        pages = (mobiles_qty // 60) + 1
    all_urls = [url]
    for i in range(2, pages+1):
        all_urls.append("https://egypt.souq.com/eg-en/mobile-phone/l/?page={}".format(i))
    with open("all-links.pkl", 'wb') as file:
        pickle.dump(all_urls, file)

all_links()


def data_from_links():
	names = []
	price = []
	currency = []
    with open("all-links.pkl", 'rb') as file:
        all_urls = pickle.load(file)
    for url in all_urls:
        soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
        a = soup.find("div", {"class" : "row collapse content flex-box-grid medium-up-1 large-up-1"})        .find_all("div", {"class" : "column column-block block-list-large single-item"})
        for z in a:
            names.append(''.join([i for i in z.find("div", {"class" : "col col-info item-content"})                     .find("a").get_text().strip() if i.isprintable()]))
            b = z.find("div", {"class" : "col col-buy"}).find("div", {"class" : "is sk-clr1"})
            c = b.find("h3").get_text()
            price.append(''.join([i for i in c[:c.find(".")] if i.isnumeric()]))
            currency.append(b.find("small", {"class" : "currency-text sk-clr1 itemCurrency"}).get_text())
    with open("names.pkl", 'wb') as file:          
    	pickle.dump(names, file)
    with open("price.pkl", 'wb') as file:          
    	pickle.dump(price, file)
    with open("currency.pkl", 'wb') as file:       
    	pickle.dump(currency, file)
data_from_links()

def list_to_csv():
    with open("names.pkl", 'rb') as file:          
        mobiles = pickle.load(file)
    with open("price.pkl", 'rb') as file:          
        prices = pickle.load(file)
    with open("currency.pkl", 'rb') as file:       
        currency = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency', 'new_used']:
        df[i] = eval(i)
    df['web'] = [url]*len(mobiles)
    df.to_csv("data-souq-egypt-new.csv", index=False)
list_to_csv()
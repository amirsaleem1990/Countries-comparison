import pandas as pd
import os
import pickle
import requests
from bs4 import BeautifulSoup

url = "https://www.bestmobile.pk/used-mobiles"

def get_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    links = [url]
    pages = int(soup.find("ul", {"class" : "pagination pagination-lg"}).find_all("li")[-2].get_text())
    for i in range(2, pages+1):
        links.append(url + "/page/" + str(i))
    with open("links.pkl", "wb") as file:
        pickle.dump(links, file)
get_links()

def links_to_data():
    names = []
    prices = []
    currency = []
    errors = []
    for link in links:
        soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
        a = soup.find("div", {"class" : "classified-list"}).find_all("div", {"class" : "media"})
        for b in a:
            n = b.find("h4", {"class" : "media-heading"}).get_text().strip()
            p = b.find("strong", {"class" : "ribbon-content"}).get_text()
            if not "sold" in p.lower():
                c = p.split()[0].replace(".", "")
                try:
                    pp = int(p.split()[1].replace(",", ""))   
                except:
                    pp = p.split()[1].replace(",", "")
                names.append(n)
                prices.append(pp)
                currency.append(c)
    with open("names.pkl", "wb") as file:
        pickle.dump(names, file)
    with open("prices.pkl", "wb") as file:
        pickle.dump(prices, file)
    with open("currency.pkl", "wb") as file:
        pickle.dump(currency, file)
links_to_data()

def data_to_csv():
    with open("names.pkl", "rb") as file: 
        mobiles = pickle.load(file)
    with open("prices.pkl", "rb") as file: 
        prices = pickle.load(file)
    with open("currency.pkl", "rb") as file: 
        currency = pickle.load(file)
    web = [url]*len(mobiles)
    new_used = ['used']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency', 'new_used', 'web']:
        df[i] = eval(i)
    df.to_csv("data-bestmobile-pak-used.csv", index=False)
data_to_csv()
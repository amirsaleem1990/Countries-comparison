import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import pickle

url = "https://yaoota.com/en-eg/category/mobiles-and-tablets/mobiles/"

def get_all_links():
    url1 = url + "?category=374"
    url2 = url + "?category=375"
    links = [url1, url2]
    e = 0
    while True:
        e += 1
        print(e, end=", ")
        if not requests.get(url1 + "&page= " + str(e),timeout=5).history:
            links.append(url1 + "&page= " + str(e))
        else:
            break
    e = 0
    while True:
        e += 1
        print(e, end=", ")
        if not requests.get(url2 + "&page= " + str(e),timeout=5).history:
            links.append(url2 + "&page= " + str(e))
        else:
            break
    with open("all_links.pkl", "wb") as file:
        pickle.dump(links, file)
get_all_links()

def links_to_data():
    names = []
    cc = []
    currency = []
    price = []
    with open("all_links.pkl", 'rb') as file:
        all_links = pickle.load(file)

    for link in all_links:
        soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
        a = soup.find("div", {"class" : "search__container__result__products"}).                    find_all("div", {"class" : "search__container__result__products__single media hasProductRating"})
        for b in a:
            n = b.find("div", {"class" : "media-body"}).find("h4", {"class" : "search__container__result__products__single__title media-heading"}).                        get_text().strip()
            c = b.find("div", {"class" : "media-right hidden-xs"}).find("div", {"class" : "price-box"}).                    find("h3", {"class" : "search__container__result__products__single__price"}).get_text().strip()
            names.append(n)
            cc.append(c)

    for i in cc:
        currency.append(i.split()[-1].strip())
        price.append(float(i.split()[0].strip().replace(",", "")))
    with open("names.pkl", "wb") as file:
        pickle.dump(names, file)
    with open("currency.pkl", "wb") as file:
        pickle.dump(currency, file)
    with open("price.pkl", "wb") as file:
        pickle.dump(price, file)
links_to_data()

def data_to_csv():
    with open("names.pkl", "rb") as file:
        mobiles = pickle.load(file)
    with open("currency.pkl", "rb") as file:
        currency = pickle.load(file)
    with open("price.pkl", "rb") as file:
        prices = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency', 'new_used']:
        df[i] = eval(i)
    df['web'] = [url]*len(mobiles)
    df.to_csv("data-yaoota-egypt-new.csv", index=False)
data_to_csv()
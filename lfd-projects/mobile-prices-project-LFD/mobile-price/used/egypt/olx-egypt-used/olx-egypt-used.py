import requests
import re
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pickle

url = "https://olx.com.eg/en/mobile-phones-accessories/mobile-phones/"

def get_all_pages_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    pages = int(soup.find("div", {"class" : "pager rel clr"}).                find_all("span", {"class" : "item fleft"})[-1].get_text().replace("\n", ""))
    all_pages = [url]
    for i in range(2, pages+1):
        all_pages.append(url + "?page=" + str(i))
    with open("all_links.pkl", "wb") as file:
        pickle.dump(all_pages, file)
get_all_pages_links()


def links_to_data():
    with open("all_links.pkl", "rb") as file:
        all_pages = pickle.load(file)
    names = []
    currency = []
    prices = []
    for e, link in enumerate(all_pages):
        print(round(e/len(all_pages), 2), end=", ")
        n1 = []
        cc1 = []
        p1 = []
        try:
            soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
            a = soup.find("div", {"class" : "rel listHandler"})
            f = a.find("table", {"class" : "fixed offers breakword"}).find("tbody").find_all("tr")[-1]
            featured_ads = f.find_all("div", {"class" : "ads__item"})
            for f_add in featured_ads:
                b = f_add.find("div", {"class" : "ads__item__info"})
                n = b.find("a", {"class" : "ads__item__title"}).get_text().strip()
                c = b.find_all("p")[0].get_text().strip()
                cc = c.split()[-1]
                p = c.split()[0].replace(",", "")
                n1.append(n)
                cc1.append(cc)
                p1.append(p)
            normal_adds = a.find("table", {"id" : "offers_table"}).find("tbody").                    find("div", {"class" : "ads ads--list"}).find_all("div", {"class" : "ads__item"})
            
            for n_adds in normal_adds:
                b_2 = n_adds.find("div", {"class" : "ads__item__info"})
                n_2 = b_2.find("a", {"class" : "ads__item__title"}).get_text().strip()
                c_2 = b_2.find_all("p")[0].get_text().strip()
                cc_2 = c_2.split()[-1]
                p_2 = c_2.split()[0].replace(",", "")
                n1.append(n_2)
                cc1.append(cc_2)
                p1.append(p_2)
            names += [n_2]
            currency += [cc_2]
            prices += [p_2]
        
        except:
            pass
    with open("names_.pkl", "wb") as file:
        pickle.dump(names, file)
    with open("currency_.pkl", "wb") as file:
        pickle.dump(currency, file)
    with open("prices_.pkl", "wb") as file:
        pickle.dump(prices, file)    
links_to_data()

def data_to_csv():
    with open("names_.pkl", "rb") as file:
        mobiles = pickle.load(file)
    with open("currency_.pkl", "rb") as file:
        currency = pickle.load(file)
    with open("prices_.pkl", "rb") as file:
        prices = pickle.load(file)
    web = [url]*len(mobiles)
    new_used = ['used']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency','new_used', 'web']:
        df[i] = eval(i)
    df.to_csv("data-olx-egypt-used.csv", index=False)
data_to_csv()
import requests
from bs4 import BeautifulSoup
import pickle
import os
import pandas as pd

url = "https://www.ennap.com/mobiles-tablets-mobile-phones-1"
def all_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    a = soup.find("ul", {"class" : "nav nav-pills nav-stacked"}).find_all("li")
    links = [url]
    results_qty = int(soup.find_all("span", {"class" : "xt_product_count"})[-1].get_text())
    if results_qty % 20 == 0:
        q = results_qty
    else:
        q = int(results_qty / 20) + 1
    for i in range(2, q+1):
        links.append(url + "/page/" + str(i))
    with open("all_links.pkl", "wb") as file:
        pickle.dump(links, file)
all_links()

def data_from_links():
    currency = []
    mobiles = []
    price = []
    with open("all_links.pkl", 'rb') as file:
        links = pickle.load(file)
    for url in links:
        soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
        a = soup.find("div", {"id" : "products_grid"}).        select("div", {"class" : "oe_product oe_list oe_product_cart list-view-css"})
        for i in a:
            try:
                b = i.find("div", {"itemscope" : "itemscope"}).find("section")
                m = b.find("a").get_text()
                c = b.find("span", {"data-oe-type" : "monetary"}).get_text()
                p = ''.join([i for i in c if i.isnumeric()])
                cc = c.split()[0]
                mobiles.append(m)
                price.append(p)
                currency.append(cc)
            except:
                pass
    url = [url]*len(mobiles)
    with open("mobiles.pkl", "wb") as file:
        pickle.dump(mobiles, file)
    with open("prices.pkl", "wb") as file:
        pickle.dump(price, file)
    with open("currency.pkl", "wb") as file:
        pickle.dump(currency, file)
    with open("url.pkl", "wb") as file:
        pickle.dump(url, file)
data_from_links()


def data_to_csv():
    with open("mobiles.pkl", "rb") as file:
        mobiles = pickle.load(file)
    with open("prices.pkl", "rb") as file:
        prices = pickle.load(file)
    with open("currency.pkl", "rb") as file:
        currency = pickle.load(file)
    with open("url.pkl", "rb") as file:
        web = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles','prices','currency', 'new_used','web']:
        df[i] = eval(i)
    df.to_csv("ennap-new-egypt.csv", index=False)
data_to_csv()


import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle

url = "https://www.jumia.com.eg/mobile-phones/"
def extract_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    a = soup.find("form", {"class" : "facet-form"})
    b = a.find_all("div", {"class":"facet-el"})
    links = []
    links =  [i.find("a")['href'] for i in b]
    with open("links.pkl", "wb") as file:
    	pickle.dump(links,file)
extract_links()

def all_pages():
    with open("links.pkl", "rb") as file:
        links = pickle.load(file)
    d = {}
    for link in links:
        soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
        try:
            a = soup.find("ul", {"class" : "osh-pagination -horizontal"})
            base_link = a.find("a")['href'] + "?page={}"
            d[link.split("/")[-2]] = [base_link.format(i) for i in range(1, int(a.get_text().strip()[-1])+1)]
        except:
            d[link.split("/")[-2]] = [link]
    with open("all_pages.pkl", "wb") as file:
    	pickle.dump(d, file)
all_pages()

def extract_data():
    mobiles = []
    prices = []
    currency = []
    with open("all_pages.pkl", "rb") as file:
        dd = pickle.load(file)
    for link in [z for i in list(dd.values()) for z in i]:
        try:
            soup = BeautifulSoup(requests.get(link,timeout=5).text, "lxml")
            a = soup.find("section", {"class" : "products -mabaya"}).find_all("div", {"class" : "sku -gallery"})
            a += soup.find("section", {"class" : "products -mabaya"}).find_all("div", {"class" : "sku -gallery -has-offers"})
            for i in a:
                n = i.find("span", {"class" : "name"}).get_text()
                p = i.find("span", {"class" : "price"}).get_text()
                c = p[:p.find(" ")]
                p = ''.join([i for i in p[p.find(" "):].strip() if i.isnumeric()])
                mobiles.append(n)
                prices.append(p)
                currency.append(c)
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
extract_data()


def lists_to_csv():
    with open("mobiles.pkl", "rb") as file:
        mobiles = pickle.load(file)
    with open("prices.pkl", "rb") as file:
        prices = pickle.load(file)
    with open("currency.pkl", "rb") as file:
        currency = pickle.load(file)
    with open("web.pkl", "rb") as file:
        web = pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles','prices','currency', 'new_used','web']:
        df[i] = eval(i)
    df.to_csv("jumia-egypt-new.csv", index=False)
lists_to_csv()
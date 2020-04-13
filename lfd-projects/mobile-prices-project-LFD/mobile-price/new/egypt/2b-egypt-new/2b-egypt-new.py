import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle

url = "https://2b.com.eg/en/mobile-and-tablet/mobiles.html"
def first_level_links():
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    a = soup.find("div", {"class" : "filter-options-content"}).find("ol", {"class" : "items"}).find_all("li")
    d = {}
    for i in a:
        b = ''.join([i for i in i.find("a").get_text().strip() if i and not i.isnumeric()])
        d[b[:b.find(" ")]] = i.find("a")['href']
    with open("1st-level-links.pkl", 'wb') as file:
        pickle.dump(d, file)
first_level_links()

def mobiles_and_prices():
    with open("1st-level-links.pkl", 'rb') as file:
        d = pickle.load(file)
    mobiles = []
    prices = []
    currency = []
    for url in d.values():
        try:
            soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
            a = soup.find("ol", {"class" : "products list items product-items"}).find_all("li")
            for mobile in a:
                m = mobile.find("a", {"class" : "product-item-link"}).get_text().strip()
                cp = mobile.find("span", {"class" : "price"}).get_text()
                p = ''.join([i for i in cp if i.isnumeric()])
                c = cp[:cp.find(re.findall(r'\d+', cp)[0])]
                prices.append(p)
                currency.append(c)
                mobiles.append(m)
        except:
            pass
    with open("mobiles.pkl", 'wb') as file:
        pickle.dump(mobiles, file)
    with open("prices.pkl", 'wb') as file:
        pickle.dump(prices, file)
    with open("currency.pkl", 'wb') as file:
        pickle.dump(currency, file)
mobiles_and_prices()

def list_to_csv():
    with open("mobiles.pkl", 'rb') as file:
        mobiles = pickle.load(file)
    with open("prices.pkl", 'rb') as file:
        prices = pickle.load(file)
    with open("currency.pkl", 'rb') as file:
        currency = pickle.load(file)
    df = pd.DataFrame()
    new_used = ['new']*len(mobiles)
    for i in ['mobiles', 'prices', 'currency', 'new_used']:
        df[i] = eval(i)
    df['web'] = url
    df.to_csv("2b-new-egypt.csv", index=False)
list_to_csv()
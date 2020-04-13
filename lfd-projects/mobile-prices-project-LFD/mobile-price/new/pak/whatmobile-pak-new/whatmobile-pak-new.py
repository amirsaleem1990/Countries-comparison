import requests
from bs4 import BeautifulSoup
import re
import pickle
import pandas as pd

def data_from_link():
    url = "https://www.whatmobile.com.pk/"
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    
    url = soup.find("div", {"class" : "verticalMenu"}).find_all("section")[2].find_all("li")[-1].find("a")['href']
    url = "https://www.whatmobile.com.pk/" + url
    soup = BeautifulSoup(requests.get(url,timeout=5).text, "lxml")
    a = soup.find("td", {"width" : "655"}).find_all("td", {"class" : "BiggerText"})
    mobiles = []
    prices = []
    currency = []
    for b in a:
        try:
            n = b.find("a", {"class" : "BiggerText"}).get_text().strip()
            c = b.find("span", {"class" : "PriceFont"}).get_text().strip()
            cc = c.split()[0]
            p = ''.join([i for i in c.split()[-1] if i.isnumeric()])
            mobiles.append(n)
            prices.append(p)
            currency.append(cc)
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
data_from_link()

def data_to_csv():
    with open("mobiles.pkl", "rb") as file:
        mobiles= pickle.load(file)
    with open("prices.pkl", "rb") as file:
        prices= pickle.load(file)
    with open("currency.pkl", "rb") as file:
        currency= pickle.load(file)
    with open("web.pkl", "rb") as file:
        web= pickle.load(file)
    new_used = ['new']*len(mobiles)
    df = pd.DataFrame()
    for i in ['mobiles', 'prices', 'currency','new_used', 'web']:
        df[i] = eval(i)
    df.to_csv("whatmobile-pak-new-data.csv", index=False)
data_to_csv()
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
with open("adds_links_rent.pkl", "rb") as file:
    links_rent = pickle.load(file)
all_records_list = []
errors = []
for e, url in enumerate(links_rent):
    print(e, round(e / len(links_rent), 3), end = " | ")
    try:
        soup = BeautifulSoup(requests.get(url).text, "lxml")

        a = soup.find("div", {"class" : "property-page__column"})
        left = a.find("div", {"class" : "property-page__column--left"})
        right = a.find("div", {"class" : "property-page__column--right"})

        c = [i for i in left.find("div", {"class" : "property-facts"}).text.replace("  ", "").split("\n") if i]

        d = {c[0] : c[1],
            c[2] : c[3] + c[4],
            c[5] : c[6],
            c[7] : c[8]}
        d["Price"] = ''.join([i for i in right.text.replace("  ", "").split("\n") if i][:2])
        d["address"] = left.find("div", {"class" : "panel panel--style1 property-page__agent-location-area"}).find("div", {"class" : "text text--size3"}).text
        d["Url"] = url
        all_records_list.append(d)
    except:
        errors.append(url)
        pass
df = pd.DataFrame(all_records_list)

with open("errors_rent.pkl", "wb") as file:
    pickle.dump(errors, file)
with open("df_rent.pkl", "wb") as file:
    pickle.dump(df, file)
with open("all_records_list_rent.pkl", "wb") as file:
    pickle.dump(all_records_list, file)

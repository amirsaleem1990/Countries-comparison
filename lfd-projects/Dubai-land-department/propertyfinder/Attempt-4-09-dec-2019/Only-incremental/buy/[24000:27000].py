
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle

errors = []
all_records_list = [] 

with open("links_to_scrap.pkl", "rb") as file:
	links_to_scrap = pickle.load(file)
links_to_scrap = links_to_scrap[24000:27000]
for e, url in enumerate(links_to_scrap):
	print("#:", e, "|", "%:",  round(e / len(links_to_scrap), 3), "|", "Qty errors:", len(errors), "|", "Qty. OK", len(all_records_list))
	try:
		soup = BeautifulSoup(requests.get(url).text, "lxml")
		a = soup.find("div", {"class" : "property-page__column"})
		left = a.find("div", {"class" : "property-page__column--left"})
		right = a.find("div", {"class" : "property-page__column--right"})
		b = left.find("div", {"class" : "panel panel--style1 panel--style3"}).find("div", {"class" : "property-facts"}).text
		c = [i.strip().strip(":") for i in b.replace("  ", "").split("\n") if i]
		d = {c[0] : c[1],
			c[2] : c[3] + c[4],
			c[5] : c[6],
			c[7] : c[8],
			c[9] : c[10]}
		d["Price"] = ''.join([i for i in right.text.replace("  ", "").split("\n") if i][:2])
		d["address"] = left.find("div", {"class" : "panel panel--style1 property-page__agent-location-area"}).find("div", {"class" : "text text--size3"}).text
		d["Url"] = url
		all_records_list.append(d)
	except:
		errors.append(url)
		pass
df = pd.DataFrame(all_records_list)
with open("errors_buy[24000:27000].pkl", "wb") as file:
	pickle.dump(errors, file)
with open("df_buy.pkl[24000:27000]", "wb") as file:
	pickle.dump(df, file)
with open("all_records_list_buy[24000:27000].pkl", "wb") as file:
	pickle.dump(all_records_list, file)

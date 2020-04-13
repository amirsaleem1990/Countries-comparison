import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
with open("adds_links_rent.pkl", "rb") as file:
	links_rent = pickle.load(file)

with open("priveious_links_rent.pkl", "rb") as file:
	priveious_links = pickle.load(file)

links_to_scrap = list(set([i for i in links_rent if not i in priveious_links]))
print(f"There are {len(links_to_scrap)} adds to scrap")
with open("links_to_scrap.pkl", "wb") as file:
	pickle.dump(links_to_scrap, file)


script = """
import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
with open("links_to_scrap.pkl", "rb") as file:
	links_to_scrap = pickle.load(file)
links_to_scrap = links_to_scrapabcdef
all_records_list = []
errors = []
for e, url in enumerate(links_to_scrap):
	print("#:", e, "|", "%:",  round(e / len(links_to_scrap), 3), "|", "Qty errors:", len(errors), "|", "Qty. OK", len(all_records_list))
	try:
		soup = BeautifulSoup(requests.get(url).text, "lxml")

		a = soup.find("div", {"class" : "property-page__column"})
		left = a.find("div", {"class" : "property-page__column--left"})
		right = a.find("div", {"class" : "property-page__column--right"})

		c = [i for i in left.find("div", {"class" : "property-facts"}).text.replace("  ", "").split("\\n") if i]

		d = {c[0] : c[1],
			c[2] : c[3] + c[4],
			c[5] : c[6],
			c[7] : c[8]}
		d["Price"] = ''.join([i for i in right.text.replace("  ", "").split("\\n") if i][:2])
		d["address"] = left.find("div", {"class" : "panel panel--style1 property-page__agent-location-area"}).find("div", {"class" : "text text--size3"}).text
		d["Url"] = url
		all_records_list.append(d)
	except:
		errors.append(url)
		pass
df = pd.DataFrame(all_records_list)
print(len(df))
with open("errors_rentabcdef.pkl", "wb") as file:
	pickle.dump(errors, file)
with open("df_rentabcdef.pkl", "wb") as file:
	pickle.dump(df, file)
with open("all_records_list_rentabcdef.pkl", "wb") as file:
	pickle.dump(all_records_list, file)
"""

with open("links_to_scrap.pkl", "rb") as file:
	links_to_scrap = pickle.load(file)

for i in range(0, len(links_to_scrap) + 1, 3000):
	if ((len(links_to_scrap) + 1 - i) > 3000):
		rang = f"[{i}:{i+3000}]"
		s = script.replace("abcdef", rang)
		file_name = f"{rang}.py"
		with open(file_name, "w") as file:
			file.write(s)
	else:
		rang = f"[{i}:]"
		s = script.replace("abcdef", rang)
		file_name = f"{rang}.py"
		with open(file_name, "w") as file:
			file.write(s)


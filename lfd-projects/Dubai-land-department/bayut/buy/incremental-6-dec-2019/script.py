import pandas as pd
import requests
import pickle
from bs4 import BeautifulSoup
errors = []
import pickle

all_records_list = []

with open("../all_adds_link.pkl", "rb") as file:
	all_adds_links = pickle.load(file)
all_adds_links = list(set(all_adds_links))

with open("../all_adds_link_old_1.pkl", "rb") as file:
	old = pickle.load(file)
new_links = [i for i in all_adds_links if not i in old]
print(f"There is {len(new_links)} new links")

for e, url in enumerate(all_adds_links):
	print(e, end=", ")
	try:
		soup = BeautifulSoup(requests.get("https://www.bayut.com" + url).text, "lxml")
		a = [i.text for i in soup.find("ul", {"class" : "_033281ab"}).select("span", {"class" : "_3af7fa95"})]
		b = []
		for i in a:
			if not i.isnumeric():
				if not i in b:
					b.append(i)
			else:
				b.append(i)
		one_record_dict = dict(zip(b[::2], b[1:][::2]))
		all_records_list.append(one_record_dict)
	except:
		errors.append(url)
		pass

df = pd.DataFrame(all_records_list)

with open("df_incremental_1.pkl", "wb") as file:
	pickle.dump(df, file)
with open("errors_incremental_1.pkl", "wb") as file:
	pickle.dump(errors, file)
with open("all_records_list_incremental_1.pkl", "wb") as file:
	pickle.dump(all_records_list, file)

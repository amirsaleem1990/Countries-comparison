import requests
from bs4 import BeautifulSoup
import pandas as pd
import pickle
with open("../links-buy.pkl", "rb") as file:
    links_buy = pickle.load(file)
links_buy = ["https://www.propertyfinder.ae" + i for i in links_buy]abcdef
all_records_list = []
errors = []
for url in links_buy:
    try:
        values = []
        keys = []
        soup = BeautifulSoup(requests.get(url).text, "lxml")
        a = soup.find("div", {"class" : "facts__container"}).find("div", {"class", "facts__list"})
        for e, i in enumerate(a.find_all("div", {"class" : "facts__list-item"})):
            if e != 4:
                keys.append(i.find("div", {"class" : "facts__label"}).text.strip())
        for e, i in enumerate(a.find_all("div", {"class" : "facts__content"})):
            if e != 3:
                values.append(i.text.strip().replace("\n", "").replace("  ", ""))
        
        b = soup.find("div", {"class" : "amenities__list"})
        amenities = list(set([i.text.strip() for i in b.select("div", {"class" : "amenities__list-item"})]))
        str_amenities = "{"
        for i in amenities:
            str_amenities += i + ","
        str_amenities += "}"
        str_amenities = str_amenities.replace(",}", "}")
        price = soup.find("span", {"class" : "facts__content--price-value"}).text
        phone = int(''.join([i for i in str(soup)[str(soup).find("tel:"): str(soup).find("tel:")+20] if i.isnumeric() or i == "+"]))
        
        one_record_dict = dict(zip(keys, values))
        one_record_dict['url'] = url
        one_record_dict['amenities'] = str_amenities
        one_record_dict['price'] = price
        one_record_dict['phone'] = phone
        all_records_list.append(one_record_dict)
    except:
        errors.append(url)
        pass
df = pd.DataFrame(all_records_list)

with open("errorsabcdef.pkl", "wb") as file:
    pickle.dump(errors, file)
with open("dfabcdef.pkl", "wb") as file:
    pickle.dump(df, file)
with open("all_records_listabcdef.pkl", "wb") as file:
    pickle.dump(all_records_list, file)
import pandas as pd
import requests
from bs4 import BeautifulSoup

urls = ["https://www.justproperty.com/en/agents/"]
for i in range(2, 98):
    urls.append(f"https://www.justproperty.com/en/agents/?page={i}")
all_data = []
for url in urls:
    soup = BeautifulSoup(requests.get(url).text, "lxml")
    a = soup.find("div", {"class" : "body"}).select("div", {"class" : "item"})
    for zz in a:
        try:
            b = zz.find("div", {"class" : "columns cols-4-12 cols-m-12-12 cols-t-4-12 company-contact-info"})
            c = b.find("div", {"class" : "buttons"}).a
            d = c.attrs

            hrefs = zz.find("div", {"class" : "details-contact-info-links"}).select("a")
            dict_href_rent_and_sale = {}
            for i in hrefs:
                dict_href_rent_and_sale[i.text.split()[0].strip()] = "https://www.justproperty.com" + i['href']

            company_id   = int(eval(c['data-stats'])['company_id'])
            phone_number = int(c['data-phone'].replace("'", ""))
            company_name = zz.find("div", {"class" : "columns cols-6-12 cols-m-12-12 cols-t-8-12 company-info"}).find("h1").text.strip()
            company_url  = "https://www.justproperty.com" + zz.find("div", {"class" : "columns cols-6-12 cols-m-12-12 cols-t-8-12 company-info"}).find("h1").find("a")['href']
            soup_company = BeautifulSoup(requests.get(company_url).text, "lxml")
            soup_company.find("div", {"class" : "columns cols-4-12 cols-m-12-12 cols-t-4-12 company-contact-info"}).\
                            find("div", {"class" : "address"}).text.strip()
            LatLng = str(soup_company)[str(soup_company).index("new google.maps.LatLng"): str(soup_company).index("new google.maps.LatLng")+70].replace("new google.maps.LatLng(", "")
            LatLng = [float(i.strip()) for i in LatLng[:LatLng.index(");")].split(",")]

            company_info_dict = {}
            company_info_dict['phone number'] = phone_number
            company_info_dict['company id']   = company_id
            company_info_dict['Rental link']  = dict_href_rent_and_sale['Rental']
            company_info_dict['Sales link']   = dict_href_rent_and_sale['Sales']
            company_info_dict['company name'] = company_name
            company_info_dict['company_url']  = company_url
            company_info_dict['LatLng']       = LatLng
            if not company_info_dict in all_data:
                all_data.append(company_info_dict)
        except:
            pass
df = pd.DataFrame(all_data)
df.to_csv("justproperty.csv", index=False)

import pickle
with open("justproperty.pkl", "wb") as file:
    pickle.dump(all_data, file)
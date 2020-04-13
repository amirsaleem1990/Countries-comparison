import requests
from bs4 import BeautifulSoup
import pandas as pd
url = "https://m.rbi.org.in/Scripts/BS_NBFCList.aspx"
r = requests.get(url)
if r.ok:
    html = r.text
    soup = BeautifulSoup(html, "lxml")    
    a = soup.find("table", {"class" : "tablebg"}).findAll("tr")
    title = []
    links = []
    for i in a:
        try:
            links.append(i.find("a")['href'])
        except: 
            pass
    links = [i for i in links if i.startswith("http:")]
    for i in links:
        print(i)

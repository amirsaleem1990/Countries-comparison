import pandas as pd
import requests
from bs4 import BeautifulSoup

def func(url):
    r = requests.get(url)
    html = r.text
    soup = BeautifulSoup(html, "lxml")
    names = []
    des = []
    qq = "Deputy Prime Minister"
    s = "Prime Minister of"
    t = "Deputy Minister of"
    h = "Head of the State"
    q = "Chairman of State"
    z = "Minister of"
    f = "First Deputy Prime Minister"
    a = soup.find("div", {"class" : "divNodeContent"}).findAll("p")
    for i in a[1:]:
        im = i.get_text().strip().replace("\t", "").replace("\n", "")
        if im:
            if f in im:
                ii = im.split(f); names.append(ii[0].strip()); des.append(f + ii[1].strip()); a.remove(i)
            elif qq in im:
                ii = im.split(qq);  names.append(ii[0].strip());  des.append(qq + ii[1].strip());  a.remove(i) 
            elif t in im:
                ii = im.split(t); names.append(ii[0].strip()); des.append(t + ii[1].strip()); a.remove(i)
            elif s in im:
                ii = im.split(s); names.append(ii[0].strip()); des.append(s + ii[1].strip()); a.remove(i)
            elif h in im:
                ii = im.split(h); names.append(ii[0].strip()); des.append(h + ii[1].strip()); a.remove(i)
            elif q in im:
                ii = im.split(q); names.append(ii[0].strip()); des.append(q + ii[1].strip()); a.remove(i)    
            elif z in im:
                ii = im.split(z); names.append(ii[0].strip()); des.append(z + ii[1].strip()); a.remove(i) 
    df = pd.DataFrame()
    df['name'] = names
    df['designation'] = des
    df['country'] = 'Tajikistan'
    return df

url = "http://www.president.tj/en/taxonomy/term/5/35#aini"
func(url)
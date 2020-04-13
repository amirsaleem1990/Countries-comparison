import wikipedia
import requests
from bs4 import BeautifulSoup
f_blank = []
s_blank = []
final_blank = []
base_url = "https://wikipedia.org"

def first_page_links(url): 
    first_paage_links = []
    r = requests.get(url)
    if r.ok:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        a = soup.find("div", {"class": "mw-category"})
        for i in a:
            first = i.findAll("div")
            for ii in first:
                try:
                    first_paage_links.append(base_url + ii.find("a")['href'])
                except:
                    pass
    [first_paage_links.remove(i) for i in first_paage_links if first_paage_links.count(i) > 1]
    return first_paage_links

url = 'https://wikipedia.org/wiki/Category:Lists_of_ambassadors_of_Pakistan'
def second_page_links(url):
    second_paage_links = []
    r = requests.get(url)
    if r.ok:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        try:
            a = soup.find("div", {"class" : "mw-category"}).findAll("div")
            for i in a:
                try:
                    b = i.findAll("a")
                    for z in b:
                        second_paage_links.append(base_url + z['href'])
                except:
                    pass
        except:
            pass
    [second_paage_links.remove(i) for i in second_paage_links if second_paage_links.count(i) > 1]
    return second_paage_links

def final(url):
    final_names = []
    r = requests.get(url)
    if r.ok:
        html = r.text
        soup = BeautifulSoup(html, "lxml")
        try:
            a = soup.find("div", {"class" : "mw-parser-output"}).find("table", {"class" : "wikitable sortable"})
            rows = a.findAll("tr")
            for i in rows:
                aa = i.findAll("td")
                row = [z.get_text() for z in aa]
                if row: 
                    final_names.append(row[1].strip())
        except:
            pass
    return final_names




f = first_page_links("https://en.wikipedia.org/wiki/Category:Ambassadors_of_Pakistan")

s = []
for i in f[:1]:
    t = second_page_links(i)
    if t:
        for zz in t:
            if not zz in s:
                s.append(zz)
    else:
        f_blank.append(i)


ff = []
for i in s:
    t = final(i)
    if t:
        for zz in t:
            if not zz in ff:
                ff.append(zz)
    else:
        final_blank.append(i)
def wiki_persnol_informations(soup):
    # r  = requests.get(url)
    # if r.ok:
    #     soup = BeautifulSoup(r.text, 'lxml')
    #     infoboxx.append("infobox" in str(soup))
    # else:
    #     link_not_exist.append(url)
    #     import sys
    #     pirnt('______________________________________________________________________________________________________________________')
    #     sys.exit(0)
    try:
        info = soup.find("table", { "class" : "infobox vcard" }).select('tr')
    except:
        info = soup.find("table", { "class" : "infobox biography vcard" }).select('tr')
    d = {}
    for i in range(len(info)):
        try: 
            if 'background' in str(info[i].select_one('th')):
                a = info[i].select_one('th').text
                d[a.lower().replace(',', '|')] = 'th  background'
            else:    
                a = info[i].select_one('th').text
                d[a.lower().replace(',', '|')] = 'th'
            a = ''
        except:
            pass
        try:
            b = info[i].select_one('td').text
            d[b.lower().replace(',', '|')] = 'td'
            b = ''
        except: pass
    try:
        d.pop('\n')
        d.pip(' ')
    except:
        pass

    df = pd.DataFrame()
    key = []
    value = []
    for k,v in d.items():
        if not k.startswith('signat'):
            key.append(k)
            value.append(v)
    key = [i.replace(',', '|') for i in key]
    df['key'] = key
    df['value'] = value

    start = df[df['key'].str.startswith('personal')]
    start_1 = start.index[0]+1
    if len(start.index) > 1:
        last_1 = start.index[-1]+1
    else:
        last_1 = len(df)
    last_2 = df.iloc[last_1:]
    if len(start.index) > 1:
        last = last_2[last_2['value'] == 'th  background'].index[0]
	    persnol_info = df.iloc[start_1: last_1]
    if len(start.index) > 1:
        persnol_info.drop(persnol_info[persnol_info['value'].str.endswith('background')].index[0], inplace=True)

    
    final_1 = pd.DataFrame()
    p = persnol_info
    th = list(p[p['value'] == 'th']['key'])
    td = list(p[p['value'] == 'td']['key'])
    final_1['th'] = th
    final_1['td'] = td
    final_1 = final_1.T
    return final_1




import os
import pandas as pd
from bs4 import BeautifulSoup
import numpy as np
import pickle
import grequests


links = open('urls.txt', 'r').read().splitlines()
reqs = [grequests.get(link) for link in links]
resp = grequests.map(reqs)
soups = [BeautifulSoup(r.text, 'lxml') for r in resp]
infoboxx =["infobox" in str(i) for i in soups]

al_df = []
ok_urls = []
error_dict = {}
count = 0
total = len(links)


for i in soups:
	count += 1
	try:
		t = wiki_persnol_informations(i)
		al_df.append(t)
        ok_urls.append(i)
	except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        if exc_type in error_dict:
            error_dict[exc_type].append(i)
        else:
            error_dict[exc_type] = [i]
    print(round((count / total)*100, 2), end="||")		
    
df = pd.DataFrame()
for i in al_df:
    try:
        a = i
        a.columns = a.iloc[0]
        a = a[1:]
        df = df.append(a)
    except:
        pass
df.to_csv("final.csv", index=False)
df.shape # (108, 52)
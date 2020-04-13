def wiki_persnol_informations(url):
    r  = requests.get(url)
    if r.ok:
        soup = BeautifulSoup(r.text, 'lxml')
        infoboxx.append("infobox" in str(soup))
    else:
        link_not_exist.append(url)
        import sys
        pirnt('______________________________________________________________________________________________________________________')
        sys.exit(0)
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
import requests
from bs4 import BeautifulSoup
import numpy as np
import sys
import pickle

urls = open("urls.txt", "r").read().splitlines()
infoboxx = []
link_not_exist = []
al_df = []
ok_urls = []
error_dict = {}
count = 0
total = len(urls)
for i in urls:
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
#     print('Ho gay: |{}|\t Baqi hen: |{}|\t {}'.format(count, total - count, round((count / total)*100, 2)), '\t', len(ok_urls))

with open('exist_and_no_error_but_notscraped_yet_2.txt', 'w') as file:
    file.write('\n'.join([i for i in urls if i not in ok_urls]))

with open('error_type_script_pakistan_2.', 'wb') as file:
    pickle.dump(error_dict, file)

with open("infobox.pkl", "wb") as file:
    pickle.dump(infoboxx, file)

with open("al_df.pkl", "wb") as file:
    pickle.dump(al_df, file)
    
with open("ok_urls.pkl", "wb") as file:
    pickle.dump(ok_urls, file)

with open("link_not_exist.pkl", "wb") as file:
    pickle.dump(link_not_exist, file)

    
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
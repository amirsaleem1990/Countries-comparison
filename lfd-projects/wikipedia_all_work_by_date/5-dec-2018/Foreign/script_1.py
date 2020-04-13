# Foreign

# names preprocessing
import pandas as pd
names = list(pd.read_excel('WorldPEPNames (3) ALL DONE!.xlsx').name)
names = sorted(list(set([str(i).lower().strip() for i in names if str(i)])))
names = [i.strip() for i in names if len(i.split()) != 1]
names2 = []
for i in names:
    if i.startswith('international') or i.startswith('world'):
        continue
    elif i.startswith('ms '):
        names2.append(i[3:])
    elif i.startswith('dr.'):
        names2.append(i[3:])
    elif i.startswith('dr '):
        names2.append(i[3:])
    elif i.startswith('mr.'):
        names2.append(i[3:])
    elif i.startswith('mr '):
        names2.append(i[3:])
    else:
        names2.append(i)
names = [i.strip() for i in names2]

# *************************************************************************************
import wikipedia
import os 
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
import pickle

def wiki_persnol_informations(name):
    global error_dict
    data = wikipedia.page(name)
    ok = False
    if data.original_title.lower() == name:
        ok = True
    else:
        error_dict['not_same_name'].append((name, data.original_title))
    if ok:
        url = data.url
        r  = requests.get(url)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
        try:
            info = soup.find("table", { "class" : "infobox vcard" }).select('tr')
        except:
            try:
                info = soup.find("table", { "class" : "infobox biography vcard" }).select('tr')
            except:
                error_dict['no infobox'].append(name)
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
            d.pop(' ')
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

        try:
            start = df[df['key'].str.startswith('personal')]
        except:
            error_dict['not_fount_personal_information'].append(name)
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
        final_1.to_csv('{}.csv'.format(data.original_title), index = False, header = False)

# ************************************************************************************
not_same_names = []

error_dict = {'not_same_name' : [],
             'no infobox' : []}
count = 0
total = len(names)
ok_names = []
for i in names:
    count += 1
    try:
        wiki_persnol_informations(i)
        ok_names.append(i)        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        if exc_type in error_dict:
            error_dict[exc_type].append(i)
        else:
            error_dict[exc_type] = [i]
    downloaded_files_count = !ls *.csv
    print('Ho gay: |{}|\t Baqi hen: |{}|\t %: {}'.format(count, total - count, round((count / total)*100, 2)),
     '\t', len(downloaded_files_count),'\t', 'error_dict: ',len([b for z in error_dict for b in error_dict[z]]), '\t',
      'not_same_names: ', len(error_dict['not_same_name']))
    
with open('all_errors.pkl', 'wb') as file:
	pickle.dump(error_dict, file)
for i in error_dict:
	if i != 'not_same_name':
		with open(str(i).replace('class', '').replace('<', '').replace('>', '').replace("'", '').strip()+'.txt', 'w') as file:	
         		file.write('\n'.join(error_dict[i])) 

df = pd.DataFrame()
for i in error_dict:
    df[str(i).replace('class', '').replace('<', '').replace('>', '').replace("'", '').strip()] = [len(error_dict[i])]
df = df.T
df.to_csv('errors_statistics.csv', header = None)

names_not_match = error_dict['not_same_name']
a, b = zip(*names_not_match)
df = pd.DataFrame()
df['orignal_name'] = list(a)
df['wikipedia_name'] = list(b)
df.to_csv('name_deffrence.csv', index = None)



import os
import pandas as pd
import wekipedia_script
from wekipedia_script import wiki_persnol_informations as wiki
import wikipedia
import requests
from bs4 import BeautifulSoup
import numpy as np
import sys
import pickle



df = pd.read_csv("output.csv", encoding = "ISO-8859-1", error_bad_lines=False)
df = df[df['URL'].str.contains("wikipedia")]
names = list(df.Name.str.strip())






# urls = []
# for i in names:
#     try:
#         urls.append(wikipedia.page(i).url)
#     except:
#         exceptions.append(i)
# with open('urls.pkl', 'wb') as file:
#     pickle.dump(urls, file)
# with open('no_url.pkl', 'wb') as file:
#     pickle.dump(exceptions, file)
    
    
    
    
with open('urls.pkl', 'rb') as file:
    urls = pickle.load(file)
with open('no_url.pkl', 'rb') as file:
    no_url = pickle.load(file)
len(no_url), len(urls), len(set(urls))







df = pd.DataFrame()
al_df = []
ok_names = []
error_dict = {}
count = 0
total = len(names)
for i in urls:
    os.system("firefox " + i)
#     count += 1
#     try:
#         t = wiki(i)
#         print(t)
#         al_df.append(t)
#         df = df.append(t)
#         ok_names.append(i)
#     except Exception as e:
#         exc_type, exc_obj, exc_tb = sys.exc_info()
#         if exc_type in error_dict:
#             error_dict[exc_type].append(i)
#         else:
#             error_dict[exc_type] = [i]
# #     print('Ho gay: |{}|\t Baqi hen: |{}|\t {}'.format(count, total - count, round((count / total)*100, 2)), '\t', len(ok_names))

# with open('exist_and_no_error_but_notscraped_yet_2.txt', 'w') as file:
#     file.write('\n'.join([i for i in names if i not in ok_names]))

# import pickle
# with open('error_type_script_pakistan_2.py', 'wb') as file:
#     pickle.dump(error_dict, file)

import os
import pickle
import pandas as pd
from datetime import datetime
import math
with open("logs.pkl", "rb") as file:
    d = pickle.load(file)
lst = []
folder_name = []
for i in d:
    lst.append(pd.DataFrame(d[i]))
    folder_name += [i]*len(d[i])
df = pd.concat(lst)
df[3] = folder_name
df.columns = ['File', 'Error', 'Folder']
df = df[['Folder', 'File', 'Error']]
df = df.reset_index()
df.drop("index", axis=1, inplace=True)
df['Time'] = [datetime.fromtimestamp(math.floor(int(i[-1])/1000)) for i in df.Folder.str.split("-")]
df['Time'] = list(map(lambda s: s[:-3], df['Time'].astype(str)))
df = df.sort_values(["Time", "Error"], ascending=False)
df.to_csv("logs.csv", index=False)
os.system("libreoffice logs.csv")
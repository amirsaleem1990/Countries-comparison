#!/usr/bin/python3

import os
import pickle
import pandas as pd
from datetime import datetime
import math

with open("logs/logs.pkl", "rb") as file:
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
df = df[~df.Error.isin(["Empty file", "File not found", "Corrupt folder error"])]
df = df.reset_index()
df.drop("index", axis=1, inplace=True)

# wo entries remove kar den jahan par file ka error nahi balky poora table ka hi error h
df.Folder = df.Folder.str.replace("_unzip", ".zip").str.replace("raw_synced_data/", "")
#df['Time'] = [datetime.fromtimestamp(math.floor(int(i[-1].strip(".zip"))/1000)) for i in df.Folder.str.split("-")]
#df['Time'] = list(map(lambda s: s[:-3], df['Time'].astype(str)))
df = df.sort_values("Error", ascending=False)
df = df.reset_index().drop("index", axis=1)
print(df)
if input("\n\nAre you need to save a file: [y|n]") == "y":
    df.to_csv("/home/amir/logs.csv", index=False)
    print("\nLogs saved in file </home/amir/logs.csv>\n")

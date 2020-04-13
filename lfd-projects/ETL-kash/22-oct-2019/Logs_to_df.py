import os
import pprint
import pickle
import pandas as pd
with open("logs.pkl", "rb") as file:
    d = pickle.load(file)
lst = []
folder_name = []
for i in d:
    lst.append(pd.DataFrame(d[i]))
    folder_name += [i]*len(d[i])
df = pd.concat(lst)
df[3] = folder_name
df.columns = ['File', 'Time', 'Error', 'Folder']
df = df[['Folder', 'File', 'Error', 'Time']]
df = df.reset_index()
df.drop("index", axis=1, inplace=True)
df = df.sort_values("Time", ascending=False)
df.to_csv("logs.csv", index=False)
os.system("libreoffice logs.csv")
import pandas as pd
import pickle
total_records = 0
import os
a = !ls *df*.pkl
lst = []
for i in a:
	with open(i, "rb") as file:
		adf = pickle.load(file)
	total_records += len(adf)
	lst.append(adf)
df = pd.concat(lst)	

columns_to_select = ["Area", "Bath(s)", "Bedroom(s)", "Location", "Price", "Purpose", "Ref. No:", "Type" ]
df = df[columns_to_select]
print("SHAPE: ", df.shape)
df.to_csv("full_df.csv", index=False)
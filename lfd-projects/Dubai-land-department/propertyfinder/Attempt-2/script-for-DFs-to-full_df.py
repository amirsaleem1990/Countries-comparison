dfs = !ls df*.pkl
import pickle
import pandas as pd
lst = []
for i in dfs:
    with open(i, "rb") as file:
        lst.append(pickle.load(file))

full_df = pd.concat(lst)
full_df.to_csv("full_df.csv",index=False)
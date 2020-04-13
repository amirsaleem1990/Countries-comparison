import pickle
import os
import pandas as pd
files = list(os.popen("ls df_rent*"))
files = [i.strip() for i in files]
all_dfs = []
for i in files:
	with open(i, "rb") as file:
		all_dfs.append(pickle.load(file))
df = pd.concat(all_dfs)
df.to_csv("final_df_rent.csv", index=False)



files = list(os.popen("ls errors_rent*"))
files = [i.strip() for i in files]
all_errors = []
for i in files:
	with open(i, "rb") as file:
		all_errors += pickle.load(file)
with open("final_errors_rent.txt", "w") as file:
	file.write("\n".join(all_errors))



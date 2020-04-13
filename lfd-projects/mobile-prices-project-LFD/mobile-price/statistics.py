import os
import pandas as pd


os.chdir("new/")

#egypt new
os.chdir("egypt/")
df = pd.DataFrame()
for i in os.listdir():
	os.chdir(i)
	csv_file = [i for i in os.listdir() if i.endswith(".csv")][0]
	df = df.append(pd.read_csv(csv_file))
	os.chdir("../")
a = df.groupby("new_used").count()
print("Egypt new : .....................  {}".format(a.values[0][0]))
print("Egypt new (unique): .............  {}".format(df.mobiles.nunique()))


#pak_new
os.chdir("../pak")
df = pd.DataFrame()
for i in os.listdir():
	os.chdir(i)
	csv_file = [i for i in os.listdir() if i.endswith(".csv")][0]
	df = df.append(pd.read_csv(csv_file))
	os.chdir("../")
a = df.groupby("new_used").count()
print("pak new : ....................... ", a.values[0][0])
print("pak new (unique): ............... ", df.mobiles.nunique())


# egypt used
os.chdir("../../used/egypt")
df = pd.DataFrame()
for i in os.listdir():
	os.chdir(i)
	csv_file = [i for i in os.listdir() if i.endswith(".csv")][0]
	df = df.append(pd.read_csv(csv_file))
	os.chdir("../")
a = df.groupby("new_used").count()
print("Egypt used : .................... ", a.values[0][0])
print("Egypt used (unique): ............ ", df.mobiles.nunique())


# pak used
os.chdir("../pak")
df = pd.DataFrame()
for i in os.listdir():
	os.chdir(i)
	csv_file = [i for i in os.listdir() if i.endswith(".csv")][0]
	df = df.append(pd.read_csv(csv_file))
	os.chdir("../")
a = df.groupby("new_used").count()
print("Pak used : ...................... ", a.values[0][0])
print("Pak used (unique): .............. ", df.mobiles.nunique())
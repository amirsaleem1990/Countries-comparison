#!/usr/bin/python3
import sys
import pickle
file_name = sys.argv[1]
a = pickle.load(open(file_name, "rb"))
print(a)
a.to_csv(file_name + ".csv", index=False)
print("cpied to" + file_name + ".csv")

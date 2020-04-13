errors = !ls errors*.pkl
import pickle
full_errors = []
for i in errors:
	with open(i, "rb") as file:
		e = pickle.load(file)
	full_errors += e
	print(len(e))

with open("full_errors.pkl", "wb") as file:
	pickle.dump(full_errors, file)
import pandas
import pickle
with open('all_entities_v2.pkl', 'rb') as file:
    e = pickle.load(file)
number, entity = zip(*e)
import pandas as pd
df = pd.DataFrame()
entity = [','.join(sorted(list(set(i)))) for i in entity]
df['Article #'] = number
df['Entity'] = entity 
df.to_csv('Entity_dataframe.csv', index = False)
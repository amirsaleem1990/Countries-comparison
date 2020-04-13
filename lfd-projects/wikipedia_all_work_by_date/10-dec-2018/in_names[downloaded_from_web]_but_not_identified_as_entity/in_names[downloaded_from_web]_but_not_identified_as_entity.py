import pandas as pd
import pickle
df = pd.read_csv('full_news_table_extracted_from_db.csv', delimiter='|')
t = list(df.full_text)
t = sorted(list(set([str(i) for i in t])))
text = ''
for i in t:
    text += i
text = list(set([i.strip().lower() for i in text.split()]))

with open('all_names_sorted_unique_title.txt', 'r') as file:
    an = file.read().splitlines()
names = list(set([i.lower() for i in lst if i in an]))



df = pd.read_csv('Entity_dataframe.csv')
a = []
for i in list(df.Entity):
    a += i.strip().lower().split(',')
entities = list(set(a))

b = []
for i in names:
    if i in text and i not in entities:
        b.append(i)
len(b)
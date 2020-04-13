import pandas as pd
import os
import re
import numpy as np
from nltk.tokenize import word_tokenize
import wikipedia
import sys
from nltk.tag.stanford import StanfordNERTagger
import pickle
df = pd.read_csv('full_news_table_extracted_from_db.csv', delimiter='|')
# df.shape
t = df.full_text
print('Total Articles{:>19}'.format(len(t)))
t =t.dropna()
print('After removing Na{:>16}'.format(len(t)))
t.drop_duplicates(inplace=True)
print('After removing duplicates{:>8}'.format(len(t)))
t = list(t)
def extract_entities(text):
    st = StanfordNERTagger("english.all.3class.distsim.crf.ser.gz","stanford-ner.jar")
    tokens = word_tokenize(text)
    tags = st.tag(tokens)
    return tags

strings = []
for b in t:
    string = ''
    a = extract_entities(b)
    for i in range(len(a)):
        if a[i][1] == 'PERSON':
            string += a[i][0] + ' '
        else:
            if string:
                if string[-1] != '_':
                    string += '_'
                else:
                    pass
    if string:
        strings.append((t.index(b), string))
print('Entities: ')
aa = []


for i in strings:
    a = i[1].split('_')
    a = [i.strip() for i in a if i]
    aa.append((i[0], a))
with open('all_entities.txt', 'w') as file:
    for i in aa:
        file.write(str(i[0]) + '\t' + ','.join(i[1]) + '\n')
with open('all_entities.pkl', 'wb') as file:
    pickle.dump(aa, file)

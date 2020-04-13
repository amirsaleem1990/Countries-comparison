import pandas as pd
from itertools import groupby
from nltk.tokenize import word_tokenize
import wikipedia
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

dic = {}
for ii in t:
    c = t.index(ii)
    netagged_words = extract_entities(ii)
    temp_list = []
    for tag, chunk in groupby(netagged_words, lambda x:x[1]):
        if tag == 'PERSON':
            if c in dic:
                dic[c].append(" ".join(w for w, t in chunk))
            else:
                dic[c] = [(" ".join(w for w, t in chunk))]

with open('all_entities_v2.txt', 'w') as file:
    for i in dic:
        file.write(str(i) + '\t' + ','.join(dic[i]) + '\n')
with open('all_entities_v2.pkl', 'wb') as file:
    pickle.dump(aa, file)
    
with open('final_dict.pkl', 'wb') as file:
    pickle.dump(dic, file)
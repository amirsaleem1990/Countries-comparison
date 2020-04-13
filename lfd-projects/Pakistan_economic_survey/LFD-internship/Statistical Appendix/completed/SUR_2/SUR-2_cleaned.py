import pandas as pd
import numpy as np
df1 = pd.read_csv('tables(each table in one csv)/1.csv', sep='\t').T
df2 = pd.read_csv('tables(each table in one csv)/2.csv', sep='\t').T
df3 = pd.read_csv('tables(each table in one csv)/3.csv', sep='\t').T
df4 = pd.read_csv('tables(each table in one csv)/4.csv', sep='\t').T
df5 = pd.read_csv('tables(each table in one csv)/5.csv', sep='\t').T
df6 = pd.read_csv('tables(each table in one csv)/6.csv', sep='\t').T
df7 = pd.read_csv('tables(each table in one csv)/7.csv', sep='\t').T
df8 = pd.read_csv('tables(each table in one csv)/8.csv', sep='\t').T
df9 = pd.read_csv('tables(each table in one csv)/9.csv', sep='\t').T
df10 = pd.read_csv('tables(each table in one csv)/10.csv', sep='\t').T
df11 = pd.read_csv('tables(each table in one csv)/11.csv', sep='\t').T
df12 = pd.read_csv('tables(each table in one csv)/12.csv', sep='\t').T
df13 = pd.read_csv('tables(each table in one csv)/13.csv', sep='\t').T
df14 = pd.read_csv('tables(each table in one csv)/14.csv', sep='\t').T
df15 = pd.read_csv('tables(each table in one csv)/15.csv', sep='\t').T
df16 = pd.read_csv('tables(each table in one csv)/16.csv', sep='\t').T
dfs = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16]
for i in [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14, df15, df16]:
    i.columns = i.iloc[0]
scnd_stap  = []
third_stap = []
lst_merged = []
merged = pd.DataFrame()
for i in range(0, len(dfs), 2):
    two = dfs[i].merge(dfs[i+1], how='outer', left_index=True, right_index=True)
    lst_merged.append(two)
    
for i in range(0, len(lst_merged), 2):
    two = lst_merged[i].merge(lst_merged[i+1], how='outer', left_index=True, right_index=True)
    scnd_stap.append(two)
    
for i in range(0, len(scnd_stap), 2):
    two = scnd_stap[i].merge(scnd_stap[i+1], how='outer', left_index=True, right_index=True)
    third_stap.append(two)

final = third_stap[0].merge(third_stap[1], how='outer', left_index=True, right_index=True)
df = df16.append(df15.append(df14.append(df13.append(df12.append(df11.append(df10.append(df9.append(df8.append(df7.append(df6.append(df5.append(df4.append(df3.append(df1.append(df2)))))))))))))))
df.index = [i.capitalize().strip() for i in df.index]
df.index = [' '.join(i.strip().split()) for i in df.index]
df = df.drop(df.loc['Fiscal year'].index)
cols = df.columns.tolist()
cols = cols[-1:] + cols[:-1]
df = df[cols]
df.replace('-', None, inplace=True)
for i in df.columns[1:]:
    df[i] = df[i].astype('float')
df.to_csv('SUR_2_cleaned.csv')
df

import numpy as np
df['Completion status'] = [i if i in ['Ready', "Off-plan"] else np.nan for i in df['Completion status']]
df.drop('Estimated Mortgage', inplace=True, axis = 1)
df.drop(df.columns[6], axis=1, inplace=True)
df.drop("price", inplace=True, axis=1)
df.drop("url", inplace=True, axis=1)
df.to_csv("full_df[for-hassan].csv", index=False)
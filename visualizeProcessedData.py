import pandas as pd
import os

df = pd.read_csv(os.path.join('results','processedData.csv'))
# data = df.to_numpy()
# print(df.columns.values[3])
data = df[df.columns.values[3]].apply(lambda x : x.split(','))
print(list(data))
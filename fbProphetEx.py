import pandas as pd
from fbprophet import Prophet
import matplotlib.pyplot as plt

#read data from csv
df = pd.read_csv('prophet_ex_data.csv')
df.head()

m = Prophet()
m.fit(df)

future = m.make_future_dataframe(periods=365)
# print(future.tail())

forecast = m.predict(future)
print(forecast.tail())

fig1 = m.plot(forecast)
plt.show()


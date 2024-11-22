from statsmodels.tsa.arima.model import ARIMA
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.tsa.stattools import adfuller
from statsmodels.tools.eval_measures import rmse
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# Gabungkan DataFrame berdasarkan 'date'
df_merged = pd.merge(airport_movements, weather, on='date', how='left')

# Group per jam
df_daily = df_merged.groupby(pd.Grouper(key='date', freq='D')).size().reset_index(name='movement_count')

# Tetapkan 'date' sebagai indeks
df_daily.set_index('date', inplace=True)

# Mengecek stasioneritas menggunakan ADF Test
result = adfuller(df_daily['movement_count'])
print(f'ADF Statistic: {result[0]}')
print(f'p-value: {result[1]}')

# Jika p-value > 0.05, maka data tidak stasioner, perlu diferensiasi
# Diferensiasi data untuk membuatnya stasioner
df_daily_diff = df_daily['movement_count'].diff().dropna()

# Membuat Model ARIMA
model = ARIMA(df_daily['movement_count'], order=(1, 1, 1))  # p=1, d=1, q=1 sebagai contoh
model_fit = model.fit()

# Memprediksi nilai masa depan
forecast_steps = 7  # prediksi 7 hari ke depan
forecast = model_fit.forecast(steps=forecast_steps)

# Visualisasi hasil prediksi
plt.figure(figsize=(12, 6))
plt.plot(df_daily.index, df_daily['movement_count'], label='Data Aktual')
plt.plot(pd.date_range(df_daily.index[-1], periods=forecast_steps + 1, freq='D')[1:], forecast, label='Forecast', color='red')
plt.title('Forecast Jumlah Pergerakan Pesawat')
plt.xlabel('Tanggal')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.legend()
plt.show()

# Model SARIMA (Seasonal ARIMA)
sarima_model = SARIMAX(df_daily['movement_count'], order=(1, 1, 1), seasonal_order=(1, 1, 1, 7))  # Musiman dengan periodik 7 hari
sarima_model_fit = sarima_model.fit()

# Prediksi dengan SARIMA
forecast_sarima = sarima_model_fit.forecast(steps=forecast_steps)

# Visualisasi hasil prediksi SARIMA
plt.figure(figsize=(12, 6))
plt.plot(df_daily.index, df_daily['movement_count'], label='Data Aktual')
plt.plot(pd.date_range(df_daily.index[-1], periods=forecast_steps + 1, freq='D')[1:], forecast_sarima, label='Forecast SARIMA', color='red')
plt.title('Forecast Jumlah Pergerakan Pesawat dengan SARIMA')
plt.xlabel('Tanggal')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.legend()
plt.show()

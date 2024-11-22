import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
# Membaca data
airport_movements = airport_df
weather = weather_df

# Konversi kolom 'date' ke tipe data datetime
airport_movements.rename(columns={'local_time': 'date'}, inplace=True)
weather.rename(columns={'timestamp': 'date'}, inplace=True)
airport_movements['date'] = pd.to_datetime(airport_movements['date'])
weather['date'] = pd.to_datetime(weather['date'])

# Gabungkan DataFrame berdasarkan 'date'
df_merged = pd.merge(airport_movements, weather, on='date', how='left')

# Group per jam
df_hourly = df_merged.groupby(pd.Grouper(key='date', freq='D')).size().reset_index(name='movement_count')

# Tetapkan 'date' sebagai indeks
df_hourly.set_index('date', inplace=True)

# Visualisasi pergerakan pesawat per jam
plt.figure(figsize=(12, 6))
plt.plot(df_hourly.index, df_hourly['movement_count'], label='Jumlah Pergerakan Pesawat per Hari', color='green')
plt.title('Jumlah Pergerakan Pesawat per Hari')
plt.xlabel('Hari')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.legend()
plt.grid()
plt.xticks(rotation=45)
plt.show()

# Plot distribusi suhu, kelembapan, dan kecepatan angin
plt.figure(figsize=(15, 7))

#  Distribusi suhu
plt.subplot(1, 3, 1)
sns.histplot(df_merged['temperature'], kde=True)
plt.title('Distribusi Suhu')

#  Distribusi kelembapan
plt.subplot(1, 3, 2)
sns.histplot(df_merged['humidity'], kde=True)
plt.title('Distribusi Kelembapan')

# Subplot 3: Distribusi kecepatan angin
plt.subplot(1, 3, 3)
sns.histplot(df_merged['wind_speed'], kde=True)
plt.title('Distribusi Kecepatan Angin')

plt.tight_layout()
plt.show()

# Correlation Heatmap
corr = df_merged[['temperature', 'humidity', 'wind_speed']].corr()
plt.figure(figsize=(8, 6))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Correlation Heatmap')
plt.show()

# Visualisasi Perbandingan Jumlah Pergerakan Pesawat Berdasarkan Cuaca
plt.figure(figsize=(10, 6))
sns.countplot(data=df_merged, x='weather_desc', hue='is_cargo', palette='Set2')
plt.title('Jumlah Pergerakan Pesawat Berdasarkan Cuaca (Cargo vs Non-Cargo)')
plt.xlabel('Cuaca')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.xticks(rotation=45)
plt.show()

#  Visualisasi Suhu vs Kecepatan Angin dan Pergerakan Pesawat
plt.figure(figsize=(10, 6))
sns.scatterplot(data=df_merged, x='temperature', y='wind_speed', hue='is_cargo', style='weather_desc', palette='Set1')
plt.title('Suhu vs Kecepatan Angin dan Pergerakan Pesawat')
plt.xlabel('Suhu (°C)')
plt.ylabel('Kecepatan Angin (m/s)')
plt.show()

# Korelasi antara variabel numerik
corr = df_merged[['temperature', 'humidity', 'wind_speed', 'is_cargo']].corr()
plt.figure(figsize=(8, 6))
sns.heatmap(corr, annot=True, cmap='coolwarm', fmt='.2f')
plt.title('Korelasi antara Variabel Cuaca dan Pergerakan Pesawat')
plt.show()

#  Jumlah Pergerakan Pesawat Berdasarkan Status Cuaca
weather_status = df_merged.groupby('weather_desc')['is_cargo'].sum()
plt.figure(figsize=(10, 6))
weather_status.plot(kind='bar', color='skyblue')
plt.title('Jumlah Pergerakan Pesawat Cargo Berdasarkan Status Cuaca')
plt.xlabel('Cuaca')
plt.ylabel('Jumlah Pergerakan Pesawat Cargo')
plt.xticks(rotation=45)
plt.show()

#  Jumlah Pergerakan Pesawat Cargo vs Non-Cargo Berdasarkan Status Cuaca
plt.figure(figsize=(12, 6))
sns.countplot(data=df_merged, x='weather_desc', hue='is_cargo', palette='Set2')
plt.title('Jumlah Pergerakan Pesawat Berdasarkan Cuaca (Cargo vs Non-Cargo)')
plt.xlabel('Cuaca')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.xticks(rotation=45)
plt.show()

#  Pergerakan Pesawat Cargo vs Non-Cargo berdasarkan Suhu dan Kecepatan Angin
plt.figure(figsize=(12, 6))
sns.scatterplot(data=df_merged, x='temperature', y='wind_speed', hue='is_cargo', style='weather_desc', palette='Set1')
plt.title('Suhu vs Kecepatan Angin dan Pergerakan Pesawat (Cargo vs Non-Cargo)')
plt.xlabel('Suhu (°C)')
plt.ylabel('Kecepatan Angin (m/s)')
plt.show()

#  Jumlah Pergerakan Pesawat Berdasarkan Waktu (Jam)
df_merged['hour'] = df_merged['date'].dt.hour
plt.figure(figsize=(12, 6))
sns.countplot(data=df_merged, x='hour', hue='is_cargo', palette='Set2')
plt.title('Jumlah Pergerakan Pesawat Berdasarkan Waktu (Jam)')
plt.xlabel('Jam')
plt.ylabel('Jumlah Pergerakan Pesawat')
plt.xticks(rotation=45)
plt.show()

#  Jumlah Pergerakan Pesawat Berdasarkan Cuaca
weather_status = df_merged.groupby('weather_desc')['is_cargo'].sum()
plt.figure(figsize=(12, 6))
weather_status.plot(kind='bar', color='skyblue')
plt.title('Jumlah Pergerakan Pesawat Cargo Berdasarkan Status Cuaca')
plt.xlabel('Cuaca')
plt.ylabel('Jumlah Pergerakan Pesawat Cargo')
plt.xticks(rotation=45)
plt.show()

#  Visualisasi Jumlah Pergerakan Pesawat Cargo Berdasarkan Maskapai dan Cuaca
airline_weather_status = df_merged.groupby(['weather_desc', 'airline_name'])['is_cargo'].sum().unstack()
airline_weather_status.plot(kind='bar', stacked=True, figsize=(12, 6), color=['lightblue', 'lightgreen','red','blue'])
plt.title('Jumlah Pergerakan Pesawat Cargo Berdasarkan Maskapai dan Cuaca')
plt.xlabel('Cuaca')
plt.ylabel('Jumlah Pergerakan Pesawat Cargo')
plt.xticks(rotation=45)
plt.show()

#  Distribusi Suhu, Kelembapan, dan Kecepatan Angin berdasarkan Cuaca
plt.figure(figsize=(12, 6))
sns.histplot(df_merged[df_merged['weather_desc'] == 'haze']['temperature'],
             kde=True, color='orange', label='Haze Temperature')
sns.histplot(df_merged[df_merged['weather_desc'] == 'few cloud']['temperature'],
             kde=True, color='yellow', label='Few Cloud Temperature')
sns.histplot(df_merged[df_merged['weather_desc'] == 'mist']['temperature'],
             kde=True, color='lightblue', label='Mist Temperature')
sns.histplot(df_merged[df_merged['weather_desc'] == 'scattered cloud']['temperature'],
             kde=True, color='gray', label='Scattered Cloud Temperature')
sns.histplot(df_merged[df_merged['weather_desc'] == 'thunderstorm']['temperature'],
             kde=True, color='purple', label='Thunderstorm Temperature')
plt.title('Distribusi Suhu Berdasarkan Cuaca')
plt.xlabel('Suhu (°C)')
plt.ylabel('Frekuensi')
plt.legend()
plt.show()

plt.figure(figsize=(12, 6))
sns.lineplot(x=df_merged['date'], y=df_merged['temperature'], marker='o', label='Temperature (°C)', color='orange')
sns.lineplot(x=df_merged['date'], y=df_merged['humidity'], marker='o', label='Humidity (%)', color='blue')
plt.title('Tren Temperatur dan Kelembapan Berdasarkan Waktu')
plt.xlabel('Date')
plt.ylabel('Value')
plt.legend()
plt.xticks(rotation=45)
plt.show()
import requests
import time
from datetime import datetime, timedelta
import psycopg2

# Fungsi untuk menghubungkan ke PostgreSQL
def connect_to_postgresql():
    return psycopg2.connect(
            
            host="localhost",
            port=5432,
            dbname="",
            user="",
            password=""
    )

# Fungsi untuk mengambil data cuaca historis
def fetch_historical_weather_data():
    """
    Mengambil data cuaca historis dari OpenWeather API setiap 12 jam.
    """
    api_key = "4c6967bad7e7f255f161a27182c5e9d6"
    lat = -6.1275  # Latitude Bandara Soetta
    lon = 106.6527  # Longitude Bandara Soetta
    base_url = "https://history.openweathermap.org/data/2.5/history/city"

    # Rentang tanggal dari 1 Oktober 2024 hingga hari ini
    start_date = datetime(2024, 10, 1)
    end_date = datetime.utcnow()
    current_date = start_date

    historical_data = []

    while current_date <= end_date:
        unix_timestamp = int(time.mktime(current_date.timetuple()))  # Konversi ke timestamp UNIX
        url = f"{base_url}?lat={lat}&lon={lon}&dt={unix_timestamp}&appid={api_key}&units=metric"

        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Periksa apakah ada data 'current'
            if 'current' in data:
                historical_data.append({
                    "temperature": data["current"]["temp"],
                    "pressure": data["current"]["pressure"],
                    "humidity": data["current"]["humidity"],
                    "wind_speed": data["current"]["wind_speed"],
                    "weather_desc": data["current"]["weather"][0]["description"],
                    "timestamp": datetime.utcfromtimestamp(data["current"]["dt"])
                })
            else:
                print(f"No 'current' data available for {current_date}.")
        else:
            print(f"Failed to fetch data for {current_date}: {response.status_code}")

        # Lanjutkan ke interval berikutnya (12 jam)
        current_date += timedelta(hours=12)

    return historical_data

# Fungsi untuk menyisipkan data ke PostgreSQL
def insert_data_to_postgresql(data, table_name, columns):
    """
    Menyisipkan data ke tabel PostgreSQL.
    """
    connection = None
    try:
        connection = connect_to_postgresql()
        cursor = connection.cursor()
        
        # Buat query INSERT
        query = f"""
        INSERT INTO {table_name} ({', '.join(columns)}) 
        VALUES ({', '.join(['%s'] * len(columns))})
        """
        
        for record in data:
            cursor.execute(query, tuple(record[col] for col in columns))
        
        connection.commit()
        print(f"Data successfully inserted into {table_name}.")
    except Exception as e:
        print(f"Error inserting data: {e}")
    finally:
        if connection:
            connection.close()

# Main execution
if __name__ == "__main__":
    table_name = "weather_data"
    columns = ["temperature", "pressure", "humidity", "wind_speed", "weather_desc", "timestamp"]

    historical_weather_data = fetch_historical_weather_data()

    if historical_weather_data:
        insert_data_to_postgresql(historical_weather_data, table_name, columns)
    else:
        print("No data fetched to insert.")

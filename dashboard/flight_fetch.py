import requests
import psycopg2
from datetime import datetime, timedelta

# Fungsi koneksi ke PostgreSQL
def connect_to_postgresql():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="",
            user="",
            password=""
        )
        print("Connected to PostgreSQL successfully.")
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        raise

# Fungsi untuk membangun URL dinamis
def build_dynamic_url_flights(base_url, airport_code, start_date, time_interval_hours=12):
    now = datetime.now()
    date_from = datetime.strptime(start_date, "%Y-%m-%dT%H:%M")  # Format input ISO8601
    date_to = now.strftime("%Y-%m-%dT%H:%M")

    if date_from > now:
        raise ValueError("Start date cannot be in the future.")

    date_from_str = date_from.strftime("%Y-%m-%dT%H:%M")

    url = (
        f"{base_url}/aedbx/aerodatabox/flights/airports/Iata/{airport_code}/"
        f"{date_from_str}/{date_to}"
        "?direction=Departure&withLeg=false&withCancelled=true&withCodeshared=true"
        "&withCargo=true&withPrivate=true&withLocation=false"
    )
    print(f"Generated API URL: {url}")
    return url

# Fungsi untuk mengambil data dari API
def fetch_flights_data(api_url):
    headers = {
        "x-magicapi-key": "cm3pfjh8z0001l5037kgz86nb"  # Ganti dengan API key Anda
    }
    try:
        response = requests.get(api_url, headers=headers)
        print(f"API Response Status: {response.status_code}")
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API Response Text: {response.text}")
            return None
    except Exception as e:
        print(f"Failed to fetch data from API: {e}")
        return None

# Fungsi untuk menyisipkan data ke PostgreSQL
def insert_flight_data(conn, data):
    try:
        with conn.cursor() as cursor:
            print(f"Inserting data: {data}")
            query = """
            INSERT INTO flight_data (
                airport_icao, airport_name, local_time, is_cargo,
                aircraft_model, airline_name
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                data["airport_icao"],
                data["airport_name"],
                data["local_time"],
                data["is_cargo"],
                data["aircraft_model"],
                data["airline_name"]
            ))
            conn.commit()
            print("Data inserted successfully.")
    except Exception as e:
        print(f"Failed to insert data into PostgreSQL: {e}")

# Fungsi utama untuk memproses data penerbangan
def process_flight_data(base_url, airport_code, start_date, time_interval_hours=12):
    """
    Mengambil data penerbangan dari API dan menyisipkannya ke PostgreSQL.
    """
    conn = connect_to_postgresql()
    try:
        # Konversi tanggal awal dan hitung rentang waktu
        start_datetime = datetime.strptime(start_date, "%Y-%m-%dT%H:%M")
        now = datetime.now()

        if start_datetime > now:
            raise ValueError("Start date cannot be in the future.")

        current_datetime = start_datetime
        while current_datetime < now:
            # Tentukan rentang waktu maksimum 12 jam
            next_datetime = current_datetime + timedelta(hours=time_interval_hours)
            if next_datetime > now:
                next_datetime = now

            # Buat URL API untuk rentang waktu saat ini
            api_url = build_dynamic_url_flights(
                base_url, 
                airport_code, 
                current_datetime.strftime("%Y-%m-%dT%H:%M"), 
                time_interval_hours
            )
            print(f"Fetching data from {current_datetime} to {next_datetime}...")

            # Ambil data penerbangan dari API
            flight_data = fetch_flights_data(api_url)

            if flight_data and "departures" in flight_data:
                for flight in flight_data["departures"]:
                    movement = flight.get("movement", {})
                    airport = movement.get("airport", {})
                    scheduled_time = movement.get("scheduledTime", {})
                    aircraft = flight.get("aircraft", {})
                    airline = flight.get("airline", {})

                    # Siapkan data untuk dimasukkan ke PostgreSQL
                    insert_flight_data(conn, {
                        "airport_icao": airport.get("icao"),
                        "airport_name": airport.get("name"),
                        "local_time": scheduled_time.get("local"),
                        "is_cargo": flight.get("isCargo"),
                        "aircraft_model": aircraft.get("model"),
                        "airline_name": airline.get("name")
                    })
                print(f"Successfully inserted {len(flight_data['departures'])} flight records.")
            else:
                print("No flight data found.")

            # Perbarui waktu saat ini ke interval berikutnya
            current_datetime = next_datetime
    except Exception as e:
        print(f"Error processing flight data: {e}")
    finally:
        conn.close()


# Main block
if __name__ == "__main__":
    # Konfigurasi
    FLIGHT_BASE_URL = "https://api.magicapi.dev/api/v1"
    AIRPORT_CODE = "CGK"  # Bandara Soetta
    START_DATE = "2024-10-01T06:00"  # Tanggal awal dalam format ISO8601

    # Proses data penerbangan
    process_flight_data(FLIGHT_BASE_URL, AIRPORT_CODE, START_DATE)
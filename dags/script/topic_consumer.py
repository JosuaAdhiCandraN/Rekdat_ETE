import os
import json
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

def connect_to_supabase():
    conn = psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT"),
        dbname=os.getenv("SUPABASE_DB"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD")
    )
    return conn

def insert_flight_data(conn, data):
    with conn.cursor() as cursor:
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

def insert_weather_data_to_postgresql(data, table_name="weather_data"):
    """
    Menyisipkan data cuaca ke tabel PostgreSQL.
    
    Args:
        data (list of dict): Data yang akan dimasukkan ke dalam tabel PostgreSQL.
        table_name (str): Nama tabel di PostgreSQL.
    """
    connection = None
    try:
        connection = connect_to_supabase()  # Sambungkan ke PostgreSQL
        cursor = connection.cursor()
        
        # Buat query INSERT 
        query = f"""
        INSERT INTO {table_name} (
            temperature, pressure, humidity, wind_speed, weather_desc, timestamp
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        # Loop melalui data dan masukkan setiap record ke tabel
        for record in data:
            cursor.execute(query, (
                record["temperature"],
                record["pressure"],
                record["humidity"],
                record["wind_speed"],
                record["weather_desc"],
                record["timestamp"]
            ))
        
        # Commit perubahan ke database
        connection.commit()
        print(f"Data successfully inserted into {table_name}.")
    
    except Exception as e:
        print(f"Error inserting data: {e}")
    
    finally:
        if connection:
            connection.close()


def consume_from_kafka():
    consumer = KafkaConsumer(
        'flight_topic',
        'weather_topic',
        bootstrap_servers=os.getenv("KAFKA_HOST", "localhost:9093"),
        group_id='flight_weather_group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    conn = connect_to_supabase()

    for message in consumer:
        data = message.value
        if message.topic == "flight_topic":
            insert_flight_data(conn, data)
        elif message.topic == "weather_topic":
            insert_weather_data_to_postgresql(conn, data)

if __name__ == "__main__":
    consume_from_kafka()

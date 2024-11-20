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
        INSERT INTO flight_data (flight_number, airport_icao, status)
        VALUES (%s, %s, %s)
        """
        cursor.execute(query, (data['flight_number'], data['airport_icao'], data['status']))
        conn.commit()

def insert_weather_data(conn, data):
    with conn.cursor() as cursor:
        query = """
        INSERT INTO weather_data (airport_icao, temperature, humidity)
        VALUES (%s, %s, %s)
        """
        cursor.execute(query, (data['airport_icao'], data['main']['temp'], data['main']['humidity']))
        conn.commit()

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
            insert_weather_data(conn, data)

if __name__ == "__main__":
    consume_from_kafka()

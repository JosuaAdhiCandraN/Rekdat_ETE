from kafka import KafkaConsumer
import psycopg2
import json

# Konfigurasi Kafka
KAFKA_BROKER = 'kafka:9092'
WEATHER_TOPIC = 'weather_data'
HOTEL_TOPIC = 'hotel_data'

# Konfigurasi PostgreSQL
DB_HOST = 'postgres'
DB_NAME = 'etl_db'
DB_USER = 'etl_user'
DB_PASSWORD = 'etl_password'

# Koneksi PostgreSQL
conn = psycopg2.connect(
    host=DB_HOST,
    database=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
cursor = conn.cursor()

# Konsumsi Kafka
weather_consumer = KafkaConsumer(
    WEATHER_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

hotel_consumer = KafkaConsumer(
    HOTEL_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Dictionary sementara
weather_data = {}

for message in weather_consumer:
    weather = message.value
    weather_data[weather['city']] = weather

for message in hotel_consumer:
    hotel = message.value
    city = hotel['city']
    if city in weather_data:
        cursor.execute("""
            INSERT INTO hotel_weather (hotel_name, city, country, temperature, weather_description)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            hotel['hotel_name'], city, hotel['country'],
            weather_data[city]['temperature'],
            weather_data[city]['description']
        ))
        conn.commit()

cursor.close()
conn.close()

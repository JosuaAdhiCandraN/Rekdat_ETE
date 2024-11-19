import requests
from kafka import KafkaProducer
import json
import time

# Konfigurasi Kafka
KAFKA_BROKER = 'kafka:9092'
TOPICS = {
    'weather': 'weather_data',
    'hotel': 'hotel_data'
}

# API Endpoint
API_URL = 'https://api.example.com/data'
API_KEY = 'your_api_key'

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    response = requests.get(API_URL, headers={'Authorization': f'Bearer {API_KEY}'})
    data = response.json()

    # Kirim data ke topik yang sesuai
    producer.send(TOPICS['weather'], data['weather'])
    producer.send(TOPICS['hotel'], data['hotels'])
    time.sleep(3600)

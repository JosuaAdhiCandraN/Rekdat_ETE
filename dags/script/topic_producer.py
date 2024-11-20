from kafka import KafkaProducer
import requests
import json
from datetime import datetime, timedelta

def fetch_flights_data(api_url):
    """Fetch airport flight data from the API."""
    headers = {
        "x-magicapi-key": "cm3pfjh8z0001l5037kgz86nb" 
    }
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch flight data: {response.status_code}")
        return None

def fetch_weather_data(api_url):
    """Fetch weather data (temperature and humidity) from the API."""
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch weather data: {response.status_code}")
        return None

def build_dynamic_url_flights(base_url, airport_code, time_interval_hours=12):
    """Build dynamic API URL for flight data."""
    now = datetime.now()
    date_from = (now - timedelta(hours=time_interval_hours)).strftime("%Y-%m-%dT%H:%M")
    date_to = now.strftime("%Y-%m-%dT%H:%M")

    url = f"{base_url}/flights/airports/Iata/{airport_code}/{date_from}/{date_to}?withLeg=false&withCancelled=true&withCodeshared=true&withCargo=true&withPrivate=true&withLocation=false"
    return url

def build_dynamic_url_weather(base_url, city):
    """Build dynamic API URL for weather data."""
    url = f"{base_url}?q={city}&appid=60d81d2a07de202f7a4dd33ded6c6b87&units=metric"
    return url

def produce_data_to_kafka(flight_base_url, weather_base_url, airport_code, city, flight_topic, weather_topic):
    """Produce flight and weather data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    flight_api_url = build_dynamic_url_flights(flight_base_url, airport_code)
    weather_api_url = build_dynamic_url_weather(weather_base_url, city)
    
    flight_data = fetch_flights_data(flight_api_url)
    weather_data = fetch_weather_data(weather_api_url)
    
    if flight_data:
        producer.send(flight_topic, value=flight_data)
        print(f"Sent flight data to topic '{flight_topic}': {flight_data}")
    
    if weather_data:
        producer.send(weather_topic, value=weather_data)
        print(f"Sent weather data to topic '{weather_topic}': {weather_data}")

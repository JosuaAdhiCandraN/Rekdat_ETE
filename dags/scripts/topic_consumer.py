from kafka import KafkaConsumer
import json

def consume_data_from_kafka(flight_topic, weather_topic):
    """Consume data from Kafka topics for flights and weather."""
    consumer = KafkaConsumer(
        flight_topic, weather_topic,
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        if message.topic == flight_topic:
            print(f"Received flight data from topic '{flight_topic}': {message.value}")
            # Process flight data here (store it, transform it, etc.)
        elif message.topic == weather_topic:
            print(f"Received weather data from topic '{weather_topic}': {message.value}")
            # Process weather data here (store it, transform it, etc.)

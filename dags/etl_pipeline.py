from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Airflow DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airport_weather_flight_data_pipeline',
    default_args=default_args,
    description='Pipeline for fetching airport flight and weather data',
    schedule_interval='@hourly',  # This will run every hour
    catchup=False
)

# Run flight and weather data producer
def run_data_producer():
    FLIGHT_BASE_URL = "https://api.magicapi.dev/api/v1/aedbx/aerodatabox"
    WEATHER_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
    AIRPORT_CODE = "CGK"  # Contoh kode bandara
    CITY = "Jakarta"  # Ganti dengan kota yang diinginkan
    FLIGHT_TOPIC = "airport_flights"
    WEATHER_TOPIC = "weather_data"
    
    produce_data_to_kafka(FLIGHT_BASE_URL, WEATHER_BASE_URL, AIRPORT_CODE, CITY, FLIGHT_TOPIC, WEATHER_TOPIC)

# Define Airflow tasks
task_data_producer = PythonOperator(
    task_id='run_data_producer',
    python_callable=run_data_producer,
    dag=dag
)

task_data_producer

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

default_args = {
    'owner': 'etl_project',
    'start_date': datetime(2024, 11, 1),
    'retries': 1
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@hourly'
)

# Task 1: Jalankan Kafka produser
run_producer = BashOperator(
    task_id='run_producer',
    bash_command='python /opt/airflow/dags/scripts/producer.py',
    dag=dag
)

# Task 2: Jalankan Kafka konsumen dan ETL
run_consumer = BashOperator(
    task_id='run_consumer',
    bash_command='python /opt/airflow/dags/scripts/consumer.py',
    dag=dag
)

run_producer >> run_consumer

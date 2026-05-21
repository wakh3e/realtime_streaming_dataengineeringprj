from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import time
from kafka import KafkaProducer


def stream_data_from_api():
    # Placeholder for the actual streaming logic
    print("Streaming data from API...")

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    return res['results'][0] # Isolates the primary user data payload matrix

# Essential Component #2: The Data Normalizer
def format_data(res):
    data = {}
    location = res['location']
    # Flattens complex nested dictionary keys into clean, flat key-value targets
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}"
    return data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1)
}

with DAG('user_automation', 
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=lambda: print("Streaming data from API...")
    )
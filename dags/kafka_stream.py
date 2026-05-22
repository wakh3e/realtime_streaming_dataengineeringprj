from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import time
from kafka import KafkaProducer


def ingest_data_api():
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
    
#converts dictionary to json binary object readable for kafka
def serializer(data):
    return json.dumps(data).encode('utf-8')


def stream_data(data):
    server = 'broker:2902' # Kafka broker address within the Docker network
    
    #kafka producer to stream data
    producer = KafkaProducer(bootstrap_servers=[server], max_block_ms=5000)
    curr_time = time.time()

    # Streaming Loop: Simulates a persistent high-velocity stream for a 60-second window per trigger
    while True:
        # Simulate streaming by sending data to Kafka every 5 seconds   
        if curr_time > time.time() + 60:
            break
        try:
            raw_response = ingest_data_api(data)
            formatted_data = format_data(raw_response)
            
            #stream data using produce.send(topic_name, data_in_binary)
            producer.send('users_created', serializer(formatted_data))
        except Exception as e:
            print(f'An error occurred: {e}')

    producer.flush() # tells producer we are good and to flush any remaining messages in the buffer we are done
    print("Data streamed to Kafka topic 'user_data'.")  

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
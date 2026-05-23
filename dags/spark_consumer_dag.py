from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'spark_kafka_consumer',
    default_args=default_args,
    description='Spark Kafka Consumer - Reads from Kafka topic and displays data',
    schedule_interval='*/10 * * * *',  # Run every 10 minutes
    catchup=False,
    tags=['streaming', 'kafka', 'spark'],
) as dag:

    # Task to run the Spark consumer via host Python/PySpark
    consumer_task = BashOperator(
        task_id='consume_kafka_stream',
        bash_command='cd /workspaces/realtime_streaming_dataengineeringprj && source venv/bin/activate && timeout 120 python dags/spark_stream.py 2>&1 || echo "Consumer task completed or timed out"',
    )

    consumer_task

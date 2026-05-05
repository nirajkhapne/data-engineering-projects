from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "kafka_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    producer = BashOperator(
        task_id="run_producer",
        bash_command="python producer/producer.py"
    )

    spark = BashOperator(
        task_id="run_spark",
        bash_command="spark-submit spark/stream.py"
    )

    producer >> spark

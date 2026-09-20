from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="product_incremental_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["kafka", "mysql", "incremental"],
) as dag:
    publish_updates = BashOperator(
        task_id="publish_product_updates",
        bash_command="cd /opt/project && python -m producer.producer",
    )

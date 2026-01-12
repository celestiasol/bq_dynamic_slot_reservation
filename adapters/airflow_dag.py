import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from core.controller import SlotController

CONFIG_PATH = "reservation_slot_config.json"

def dag_task(**kwargs):
    controller = SlotController.from_file(CONFIG_PATH)
    execution_date = kwargs.get("execution_date")
    controller.run(execution_date)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="bq_dynamic_slot_reservation",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "slots", "autoscale"],
)

adjust_task = PythonOperator(
    task_id="adjust_bigquery_slots",
    python_callable=dag_task,
    provide_context=True,
    dag=dag,
)

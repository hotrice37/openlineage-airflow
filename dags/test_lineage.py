from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='test_lineage',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
) as dag:
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_default',
        sql='CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name TEXT);'
    )
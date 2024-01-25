from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
import logging

# 데이터프레임 이용해서 SQL수행 

def my_custom_function(**kwargs):
    jdbc_hook = JdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")
    records = jdbc_hook.get_records("SELECT * FROM DEATH")
    for record in records:
        print(record)

with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    run_my_query = PythonOperator(
        task_id='task1',
        python_callable=my_custom_function
    )

run_my_query

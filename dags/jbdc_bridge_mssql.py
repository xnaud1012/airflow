
import pendulum
from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.decorators import task
import logging
import jpype
import jpype.imports
from jpype.types import *

with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:
    @task(task_id='execute')
    def execute(**kwargs):
        print('start')
        if not jpype.isJVMStarted():
            jpype.startJVM()

        Driver = jpype.JClass('com.your.jdbc.DriverClassName')
        print(Driver)
        jdbc_hook = JdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")
        print(jdbc_hook)
        records = jdbc_hook.get_records("SELECT * FROM DEATH")
        print(records)
        if records == None:
            records="sssssss"
        return records

    execute()


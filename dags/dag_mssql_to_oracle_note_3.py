from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.providers.jdbc.operators import JdbcOperator
import logging
#데이터프레임 이용해서 SQL수행 

with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:
    

    jdbc_task = JdbcOperator(
    task_id='run_sql_query',
    sql='SELECT * FROM DEATH',
    jdbc_conn_id='MSSQL_JDBC_CONN',  # Airflow에서 설정한 연결 ID
    dag=dag,
    )

    jdbc_task 
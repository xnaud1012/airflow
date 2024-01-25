
import pendulum
from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.decorators import task
with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    @task(task_id='execute')
    def my_custom_function(**kwargs):
        jdbc_hook = JdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")
        records = jdbc_hook.get_records("SELECT * FROM DEATH")
        for record in records:
            print(record)


my_custom_function()

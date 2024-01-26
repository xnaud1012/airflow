import pendulum
from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.decorators import task
import jpype
import jpype.imports
from jpype.types import *

with DAG(
    dag_id='jdbc_bridge_mssql',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    schedule='0 0 * * *',
    catchup=False
) as dag:

    @task(task_id='execute')
    def execute(**kwargs):
        if not jpype.isJVMStarted():
            jpype.startJVM()
        # JdbcHook 인스턴스 생성, 'MSSQL_JDBC_CONN'은 Airflow Connection ID입니다.
        jdbc_hook = JdbcHook(jdbc_conn_id="MSSQL_JDBC_CONN")
        
        # SQL 쿼리 실행, "DEATH" 테이블에서 상위 5개 레코드 조회
        records = jdbc_hook.get_records("SELECT TOP 5 * FROM DEATH")
        
        for record in records:
            print(record)  # 조회된 레코드 출력

        if jpype.isJVMStarted():
            jpype.shutdownJVM()

    execute()

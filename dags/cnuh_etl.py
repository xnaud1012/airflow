from airflow import DAG
from airflow.decorators import task, task_group
import pendulum
from airflow.hooks.base import BaseHook
import cx_Oracle
import logging
import pandas as pd
from airflow.operators.empty import EmptyOperator

def connect_oracle():
    rdb = BaseHook.get_connection('cnuh_oracle_conn')
    dsnStr = cx_Oracle.makedsn(rdb.host, rdb.port, rdb.schema)
    ora_con = cx_Oracle.connect(dsn=dsnStr,
                                user=rdb.login,
                                password=rdb.password,
                                encoding="UTF-8")
    return ora_con

def execute_procedure(proc_name):
    logging.info(f'************{proc_name} 작업 시작************')
    result = -1  # 기본적으로 실패로 가정
    with connect_oracle() as ora_conn:
        with ora_conn.cursor() as cursor:
            result_out = cursor.var(cx_Oracle.NUMBER)  # 결과를 저장할 변수
            try:
                cursor.callproc(proc_name, [result_out])
                result = result_out.getvalue()
            except cx_Oracle.DatabaseError as e:
                error, = e.args
                logging.error(f"Database error occurred: {error.code}, {error.message}")
                result = error.code  # 에러 코드를 결과로 설정
    return result

with DAG(
    dag_id="cnuh_etl",
    #schedule_interval="0 16 * * *",
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    @task_group(group_id='ODS_eSMART')
    def ODS_eSMART_ETL():
        with connect_oracle() as ora_conn:
            select_result_df = pd.read_sql("SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = 'ETL_DATA'", ora_conn)
            task_list = [row[0] for row in select_result_df.values.tolist()]      # list형식으로 담아짐        
            # @task() # **** print 용 ****
            # def print_tasklist(**kwargs):               
            #     return task_list           
            

            # dummy_task1 = EmptyOperator(task_id='empty_task1')

            # print_tasklist() >> dummy_task1

        @task()
        def dynamic_task(procedure_tables, **kwargs):
            return execute_procedure(f"ETL_DATA.INSERT_{procedure_tables}")

        for tbl in task_list:
            dynamic_task(tbl) # 하위 태스크 순차적으로 실행 ,,,
    
    ODS_eSMART_ETL()

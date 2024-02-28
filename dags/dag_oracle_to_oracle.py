import cx_Oracle
import logging
from airflow.hooks.base import BaseHook
import pendulum
from airflow.decorators import task
from airflow import DAG


with DAG(
    dag_id='dag_oracle_to_oracle',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:

    def connect_oracle():
        rdb = BaseHook.get_connection('oracle_xnaud')
        ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                                    user=rdb.login,
                                    password=rdb.password,
                                    encoding="UTF-8")
        return ora_con
   
    
    def convert_mssql_lob_to_string(lob_data):
        if lob_data is not None and isinstance(lob_data, cx_Oracle.LOB):
            return lob_data.read()
        return lob_data

    def get_sql(path):
        with open(path, 'r') as file:
            sqlQuery = file.read()
        return sqlQuery


    @task(task_id='execute1')
    def execute1():
        result="";

        with connect_oracle() as ora_conn:
            with ora_conn.cursor() as cursor:
                p_ippr_id = 'ID2002'  # 예시 ID
                result_out = cursor.var(cx_Oracle.NUMBER)  # 결과를 저장할 변수
                cursor.callproc("XNAUD.find_person_by_ippr_id",[p_ippr_id,result_out])
                result= result_out.getvalue()       
        logging.info(result) 
        
        return result
    
    @task(task_id='execute2')
    def execute2():        
        result="";
        with connect_oracle() as ora_conn:
            with ora_conn.cursor() as cursor:
                p_ippr_id = '105'  # 예시 ID
                result_out = cursor.var(cx_Oracle.NUMBER)  # 결과를 저장할 변수
                cursor.callproc("XNAUD.find_person_by_person_id",[p_ippr_id,result_out])
                result= result_out.getvalue()       
        logging.info(result) 
        
        return result
           
    # DAG 내에서 execute 함수 호출
    execute1() >> execute2()

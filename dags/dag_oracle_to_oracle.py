import cx_Oracle
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from airflow.hooks.base import BaseHook
import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import logging
#데이터프레임 이용해서 SQL수행 

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


    @task(task_id='execute')
    def execute():
        #base_path = os.path.dirname(__file__)
        base_path = os.path.dirname(os.path.abspath(__file__))
        oracle_path = os.path.join(base_path, 'sql/ORACLE/test.sql')
        
        #procedure = get_sql(oracle_path)
        result="";

        with connect_oracle() as ora_conn:
            with ora_conn.cursor() as cursor:
                p_ippr_id = 'ID2002'  # 예시 ID
                result_out = cursor.var(cx_Oracle.NUMBER)  # 결과를 저장할 변수
                cursor.callproc("XNAUD.find_person_by_ippr_id",[p_ippr_id,result_out])
                result= result_out.getvalue()       
        print(result) 
        
        return result
           
    # DAG 내에서 execute 함수 호출
    execute()

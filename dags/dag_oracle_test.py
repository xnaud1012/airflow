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
    dag_id='dag_oracle_test',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:

    def connect_oracle():
        #dsnStr = cx_Oracle.makedsn("10.1.3.21", "1521", "cnuhods")
        rdb = BaseHook.get_connection('cnuh_oracle_conn')
        dsnStr = cx_Oracle.makedsn(rdb.host, rdb.port, rdb.schema)
        ora_con = cx_Oracle.connect(dsn=dsnStr,
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
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/ORACLE/sysdate.sql')
        with connect_oracle() as ora_conn:
            
                select_result_df = pd.read_sql(get_sql(select_sql_path), ora_conn)
                first_row = select_result_df.iloc[0]
                logging.info(first_row)
                print(first_row)
           
    # DAG 내에서 execute 함수 호출
    execute()
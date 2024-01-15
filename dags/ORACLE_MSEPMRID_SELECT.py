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
    dag_id='ORACLE_MSEPMRID_SELECT',
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

    def connect_ms():
        ms_hook = MsSqlHook('mssql_default')
        ms_conn = ms_hook.get_conn()
        return ms_conn
    
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

        select_sql_path = os.path.join(base_path, 'sql/MSEPMRID/select.sql')
        create_sql_path = os.path.join(base_path, 'sql/MSEPMRID/create.sql')

        with connect_oracle() as ora_conn:
            try:
                select_result_df = pd.read_sql(get_sql(select_sql_path), ora_conn)
                first_row = select_result_df.iloc[0]
                logging.info(first_row)
                print(first_row)
            except Exception as e:
                try:
                    with ora_conn.cursor() as oracle_cursor:
                        oracle_cursor.execute(get_sql(create_sql_path))
                        ora_conn.commit()
                    select_result_df = pd.read_sql(get_sql(select_sql_path), ora_conn)
                    first_row = select_result_df.iloc[0]
                    logging.info(first_row)
                    print(first_row)
                except Exception as e:
                    logging.error("Error occurred: %s", e)
                    print('Error occurred:', e)

    # DAG 내에서 execute 함수 호출
    execute()

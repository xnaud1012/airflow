import cx_Oracle
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from airflow.hooks.base import BaseHook
import pendulum
from airflow.decorators import task
from airflow import DAG
import site

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

import logging
#데이터프레임 이용해서 SQL수행 

with DAG(
    dag_id='dag_mssql_to_oracle_note_2',
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
    def execute(**kwargs):
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/note/select.sql')
        create_sql_path = os.path.join(base_path, 'sql/note/create.sql')
        insert_sql_path = os.path.join(base_path, 'sql/note/insert.sql')        

 
        # Connect to MS SQL Server
        with connect_ms() as ms_conn:
            try:
                select_result_df = pd.read_sql(get_sql(select_sql_path),ms_conn)    
                if len(select_result_df)>0:               
                    try:
                        engine = create_engine("oracle+cx_oracle://", creator=connect_oracle, poolclass=NullPool)
                        chunksize = 1000
                        for start in range(0, len(select_result_df), chunksize):
                            end = min(start + chunksize, len(select_result_df))
                            select_result_df.iloc[start:end].to_sql('note', con=engine, if_exists='append'
                                                                    , index=False, chunksize=None) #index = False는 df의 인덱스를 db에 삽입X
                    except Exception as e:
                        logging.error(f'Error occurred: {e}')                  
                        raise
                else:
                    logging.info("No data returned from MS SQL Server.")
            finally:
                ms_conn.commit()
            
        

    execute()

import cx_Oracle
from contextlib import closing

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
from airflow.models import Variable
import os
import re
from airflow.hooks.base import BaseHook 
import pendulum
from airflow.decorators import task
from airflow import DAG
from flask import jsonify


import logging

with DAG(
        dag_id='dag_mssql_to_oracle',
        start_date=pendulum.datetime(2024, 01, 1, tz='Asia/Seoul'),
        schedule="*/10 * * * *",
        catchup=False
) as dag:
      
    def clean_sql_query(file_path):
        with open(file_path, 'r') as file:
            query = file.read()

        query = re.sub(r'[\t\s]+', ' ', query)
        query = re.sub(r'[\t\s]*,[\t\s]*', ', ', query)
        query = re.sub(r'[\t\s]*from[\t\s]*', ' FROM ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*where[\t\s]*', ' WHERE ', query, flags=re.IGNORECASE)
        return query
    

    @task(task_id='cleanedQuery')
    def extract_sql_query(**kwargs):
        ti = kwargs['ti']
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/select.sql')
        insert_sql_path = os.path.join(base_path, 'sql/insert.sql')
        update_sql_path = os.path.join(base_path, 'sql/update.sql')

        select_query = clean_sql_query(select_sql_path)
        insert_query = clean_sql_query(insert_sql_path)
        update_query = clean_sql_query(update_sql_path) 
        

        ti.xcom_push(key="select_query", value=select_query)
        ti.xcom_push(key="insert_query", value=insert_query)
        ti.xcom_push(key="update_query", value=update_query)


    def connect_oracle():
        rdb = BaseHook.get_connection('conn-db-oracle-custom')

        ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                            user=rdb.login,
                            password=rdb.password,
                            encoding="UTF-8")
        return ora_con
    
    def connect_mssql():
        ms_hook = MsSqlHook('conn-db-postgres-custom') 
        ms_conn = ms_hook.get_conn()  
        return ms_conn

    def convert_lob_to_string(lob_data):
        if lob_data is not None and isinstance(lob_data, cx_Oracle.LOB):
            return lob_data.read()
        return lob_data
    
    def set_last_run_time():
        Variable.set("timeStamp", pendulum.now('Asia/Seoul').to_datetime_string())

    
    @task(task_id='execute')
    def execute(**kwargs): 
        ti = kwargs['ti']        
        select_query = ti.xcom_pull(key="select_query", task_ids = 'cleanedQuery')
        insert_query = ti.xcom_pull(key="insert_query", task_ids = 'cleanedQuery')  
        update_query = ti.xcom_pull(key="update_query", task_ids = 'cleanedQuery')  

        


        with connect_oracle() as oracle_conn, connect_mssql() as postgres_conn:
            oracle_cursor = oracle_conn.cursor()
            oracle_update_cursor = oracle_conn.cursor()
            
            oracle_cursor.execute(select_query)
            columns = [col[0].lower() for col in oracle_cursor.description]
            
            with postgres_conn.cursor() as postgres_cursor:
                try:
                    while True:
                        rows = oracle_cursor.fetchmany(100)
                        if not rows:
                            break
                        
                        extracted_oracle_list = [{col: convert_lob_to_string(row[idx]) for idx, col in enumerate(columns)} for row in rows]
                        postgres_cursor.executemany(insert_query, extracted_oracle_list)
                        
                        update_params = [{'test_id': item['test_id']} for item in extracted_oracle_list]
                        oracle_update_cursor.executemany(update_query, update_params)
                    
                    postgres_conn.commit()
                except Exception as e:
                    oracle_conn.rollback()
                    postgres_conn.rollback()
                    logging.error(f"Operation failed: {e}")
                    raise e
                finally:
                    oracle_cursor.close()
                    oracle_update_cursor.close()
                    
        
    extract_sql_query()>>execute()
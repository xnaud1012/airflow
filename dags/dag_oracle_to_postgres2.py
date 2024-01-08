import cx_Oracle
from contextlib import closing
import pandas as pd
from airflow.models import Variable
import os
import re
from airflow.hooks.base import BaseHook 
import pendulum
from airflow.decorators import task
from airflow import DAG
from flask import jsonify
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

with DAG(
        dag_id='dag_oracle_to_postgres2',
        start_date=pendulum.datetime(2023, 12, 1, tz='Asia/Seoul'),
        #schedule="*/2 * * * *",
        schedule=None,
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
    
    def connect_postgres():
        pg_hook = PostgresHook('conn-db-postgres-custom') 
        post_con = pg_hook.get_conn()  
        return post_con

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


        with closing(connect_oracle().cursor()) as oracle_cursor, connect_postgres() as postgres_conn, connect_oracle().cursor() as oracle_update_cursor:
            oracle_cursor.execute(select_query)
            columns = [col[0].lower() for col in oracle_cursor.description]
            with postgres_conn.cursor() as postgres_cursor:
                while True:
                    rows = oracle_cursor.fetchmany(200)
                    if not rows:
                        break
                    extracted_oracle_list = [{col: convert_lob_to_string(row[idx]) for idx, col in enumerate(columns)} for row in rows]

                    try:
                        postgres_cursor.executemany(insert_query, extracted_oracle_list)                    
                        #update_params = [{'test_id': row[0]} for row in rows]
                        oracle_update_cursor.executemany(update_query,extracted_oracle_list)

                        postgres_conn.commit()
                        
                    except Exception as e:
                        logging.error(f"Operation failed: {e}")
                        raise e
                    connect_oracle().commit()
                    
        
    extract_sql_query()>>execute()
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
        columns=[];

        with connect_oracle() as oracle_conn: #select용 connect열기
            with oracle_conn.cursor() as oracle_select_cursor:
                oracle_select_cursor.execute(select_query)
                columns = [col[0].lower() for col in oracle_select_cursor.description] #select결과 가져오기                 

                while True:
                    rows = oracle_select_cursor.fetchmany(10000) # 10000 개씩 끊어서 작업
                    if not rows:
                        break

                    with connect_postgres() as postgres_conn, connect_oracle() as oracle_update_conn: # POSTGRES와 ORACLE UPDATE를 위한 연결
                        with postgres_conn.cursor() as postgres_cursor, oracle_update_conn.cursor() as oracle_update_cursor:
                            try:
                                extracted_oracle_list = [{col: convert_lob_to_string(row[idx]) for idx, col in enumerate(columns)} for row in rows]
                                postgres_cursor.executemany(insert_query, extracted_oracle_list)
                                
                                # 동시에 ORACLE 업데이트 한 행 체크 / postgres로 데이터 이관 성공하면 > oracle에서 이관한 데이터 update 체크
                                update_params = [{'test_id': item['test_id']} for item in extracted_oracle_list]
                                oracle_update_cursor.executemany(update_query, update_params)        
                                
                            except Exception as e:
                                postgres_conn.rollback()
                                oracle_update_conn.rollback()                                    
                            
                            finally: #성공하면 commit 한다.
                                postgres_conn.commit()
                                oracle_update_conn.commit()

        
        

                    
        
    extract_sql_query()>>execute()
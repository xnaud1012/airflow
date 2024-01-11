import cx_Oracle
import pandas as pd
from airflow.models import Variable
import os
import re
from airflow.hooks.base import BaseHook 
import pendulum
from airflow.decorators import task
from airflow import DAG
from flask import jsonify
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import logging

with DAG(
        dag_id='dag_mssql_to_oracle',
        start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
        schedule="*/2 * * * *",
        catchup=False
) as dag:
      
    def clean_sql_query(file_path):
        with open(file_path, 'r') as file:
            query = file.read()

        query = re.sub(r'[\t\s]+', ' ', query)
        query = re.sub(r'[\t\s]*,[\t\s]*', ', ', query)
        query = re.sub(r'[\t\s]*from[\t\s]*', ' FROM ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*where[\t\s]*', ' WHERE ', query, flags=re.IGNORECASE)
        # PL/SQL용 특정 키워드 처리 추가함.  
        query = re.sub(r'[\t\s]*:[\t\s]*', ' :', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*begin[\t\s]*', 'BEGIN ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*if[\t\s]*', 'IF ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*insert[\t\s]*', ' INSERT ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*into[\t\s]*', ' INTO ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*end[\t\s]*;', 'END;', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*create[\t\s]*', 'CREATE ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*table[\t\s]*', 'TABLE ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*declare[\t\s]*', 'DECLARE ', query, flags=re.IGNORECASE)

        return query
    

    @task(task_id='cleanedQuery')
    def extract_sql_query(**kwargs):
        ti = kwargs['ti']
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/ms_select.sql')
        insert_sql_path = os.path.join(base_path, 'sql/ms_insert_pl.sql')

        select_query = clean_sql_query(select_sql_path)
        insert_query = clean_sql_query(insert_sql_path)        

        ti.xcom_push(key="select_query", value=select_query)
        ti.xcom_push(key="insert_query", value=insert_query)



    def connect_oracle():
        rdb = OracleHook('conn-db-oracle-custom')
        ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                            user=rdb.login,
                            password=rdb.password,
                            encoding="UTF-8")
        return ora_con
    
    def connect_ms():
        ms_hook = MsSqlHook('mssql_default') 
        ms_conn = ms_hook.get_conn()  
        return ms_conn

    def convert_mssql_lob_to_string(lob_data): #전처리 구간 
   
        #if lob_data is not None and isinstance(lob_data, (bytes, bytearray)):
        #    return lob_data.decode('utf-8')  # 바이너리 데이터를 문자열로 변환
        return lob_data
    
    def set_last_run_time():
        Variable.set("timeStamp", pendulum.now('Asia/Seoul').to_datetime_string())

    
    @task(task_id='execute')
    def execute(**kwargs): 
        ti = kwargs['ti']        
        select_query = ti.xcom_pull(key="select_query", task_ids = 'cleanedQuery') #ms에서 select
        insert_query = ti.xcom_pull(key="insert_query", task_ids = 'cleanedQuery') #oracle로 insert 

        columns=[];

        with connect_ms() as ms_conn: #select용 connect열기
            with ms_conn.cursor() as ms_select_cursor:
                ms_select_cursor.execute(select_query)
                columns = [col[0].lower() for col in ms_select_cursor.description] #select결과 가져오기                 

                while True:
                    rows = ms_select_cursor.fetchmany(100) # n 개씩 끊어서 작업
                    if not rows:
                        break

                    with connect_oracle() as oracle_conn: # POSTGRES와 ORACLE UPDATE를 위한 연결
                        with oracle_conn.cursor() as oracle_cursor:
                            try:
                                extracted_ms_list = [{col: convert_mssql_lob_to_string(row[idx]) for idx, col in enumerate(columns)} for row in rows]
                                oracle_cursor.executemany(insert_query, extracted_ms_list)                                           
                                
                            except Exception as e:                            
                                oracle_conn.rollback()                                    
                            
                            finally: #성공하면 commit 한다.                          
                                oracle_conn.commit()
                
        
    extract_sql_query()>>execute()
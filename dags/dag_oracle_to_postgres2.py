import cx_Oracle
from contextlib import closing
import pandas as pd
import psycopg2
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
        return query
    

    @task(task_id='cleanedQuery')
    def extract_select_sql_query(**kwargs):
        ti = kwargs['ti']
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/select.sql')
        update_sql_path = os.path.join(base_path, 'sql/update.sql')

        select_query = clean_sql_query(select_sql_path)
        update_query = clean_sql_query(update_sql_path)

        ti.xcom_push(key="select_query", value=select_query)
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



    
    @task(task_id='execute')
    def execute(**kwargs): 
        ti = kwargs['ti']          

        queries = ti.xcom_pull(task_ids='cleanedQuery')       
        select_query = queries['select_query']
        update_query = queries['update_query']     

        with closing(connect_oracle().cursor()) as oracle_cursor:
            oracle_cursor.execute(select_query)
            columns=[col[0] for col in oracle_cursor.description]
            extracted_oracle_list =[]
            postgres_conn=connect_postgres()
            postgres_cursor=postgres_conn.cursor()
            try:

                while True:
                    rows = oracle_cursor.fetchmany(100) # 100줄씩 끊어서 작업 

                    if not rows:                     
                        break
                    else:
                        try:
                            extracted_oracle_list = [dict(zip(columns, row)) for row in rows]
                            postgres_cursor.executemany(update_query, extracted_oracle_list)
                            postgres_conn.commit()  # 트랜잭션 커밋
                        except Exception as e:
                            logging.error(f"Update failed: {e}")
                            return jsonify({"error": str(e)}), 500    
                        #extracted_oracle_list =[]   #초기화
                
            finally:
                postgres_conn.commit()
                postgres_cursor.close()   
        
    extract_select_sql_query()>>execute()
    
     







   



        

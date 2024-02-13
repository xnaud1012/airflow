
import pandas as pd
import os
from airflow.hooks.base import BaseHook
import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
#데이터프레임 이용해서 SQL수행 

with DAG(
    dag_id='dag_postgres_cdm_concept',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:

    def connect_db():
        pg_hook = PostgresHook('postgres_25') 
        post_con = pg_hook.get_conn()  
        print(post_con)
        return post_con



    def get_sql(path):
        with open(path, 'r') as file:
            sqlQuery = file.read()
        return sqlQuery


    @task(task_id='execute')
    def execute():
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/CDM/concept_select.sql')

        first_row =""
        with connect_db() as post_conn:
            
                select_result_df = pd.read_sql(get_sql(select_sql_path), post_conn)
                first_row = select_result_df.iloc[0]
                logging.info(first_row)
                print(first_row)
        return first_row
           
    # DAG 내에서 execute 함수 호출
    execute()

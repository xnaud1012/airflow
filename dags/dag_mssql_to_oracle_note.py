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

import logging

#inner join 도 read햘수 있는 지 확인해보기

with DAG(
    dag_id='dag_mssql_to_oracle_note',
    start_date=pendulum.datetime(2024, 1, 1, tz='Asia/Seoul'),
    #schedule="*/2 * * * *",
    schedule='0 0 * * *',
    catchup=False
) as dag:

    def clean_sql_query(file_path):
        with open(file_path, 'r') as file:
            query = file.read()

        query = re.sub(r'[\t\s]+', ' ', query)
        query = re.sub(r'[\t\s]*,[\t\s]*', ', ', query)
        query = re.sub(r'[\t\s]*and[\t\s]*', ' AND ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*or[\t\s]*', ' OR ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*from[\t\s]*', ' FROM ', query, flags=re.IGNORECASE)
        query = re.sub(r'[\t\s]*where[\t\s]*', ' WHERE ', query, flags=re.IGNORECASE)

        return query

    @task(task_id='cleanedQuery')
    def extract_sql_query(**kwargs):
        ti = kwargs['ti']
        base_path = os.path.dirname(__file__)

        select_sql_path = os.path.join(base_path, 'sql/note/select.sql')
        insert_sql_path = os.path.join(base_path, 'sql/note/insert.sql')
        create_sql_path = os.path.join(base_path, 'sql/note/create.sql')

        select_query = clean_sql_query(select_sql_path)
        insert_query = clean_sql_query(insert_sql_path)
        create_query = clean_sql_query(create_sql_path)

        ti.xcom_push(key="select_query", value=select_query)
        ti.xcom_push(key="insert_query", value=insert_query)
        ti.xcom_push(key="create_query", value=create_query)

    def connect_oracle():
        rdb = BaseHook.get_connection('conn-db-oracle-custom')
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

    def set_last_run_time():
        Variable.set("timeStamp", pendulum.now('Asia/Seoul').to_datetime_string())

    @task(task_id='execute')
    def execute(**kwargs):
        ti = kwargs['ti']
        select_query = ti.xcom_pull(key="select_query", task_ids='cleanedQuery')
        insert_query = ti.xcom_pull(key="insert_query", task_ids='cleanedQuery')
        create_query = ti.xcom_pull(key="create_query", task_ids='cleanedQuery')

        # Connect to MS SQL Server
        with connect_ms() as ms_conn:
            with ms_conn.cursor() as ms_select_cursor:
                ms_select_cursor.execute(select_query)
                columns = [col[0].lower() for col in ms_select_cursor.description]
                first_row = ms_select_cursor.fetchone()

                if first_row: # ETL할 row가 존재할 떄만 로직 
                    # 오라클 시작
                    with connect_oracle() as oracle_conn:
                        with oracle_conn.cursor() as oracle_cursor:
                            try:
                                # Create table if not exists
                                oracle_cursor.execute(create_query)
                                oracle_conn.commit()

                                # Insert first row
                                extracted_row = {col: convert_mssql_lob_to_string(first_row[idx]) for idx, col in enumerate(columns)}
                                oracle_cursor.execute(insert_query, extracted_row)

                                # Insert subsequent rows
                                while True:
                                    rows = ms_select_cursor.fetchmany(100)
                                    if not rows:
                                        break
                                    extracted_ms_list = [{col: convert_mssql_lob_to_string(row[idx]) for idx, col in enumerate(columns)} for row in rows]
                                    print(extracted_ms_list)
                                    oracle_cursor.executemany(insert_query, extracted_ms_list)

                                oracle_conn.commit()
                            except Exception as e:
                                logging.error(f'Error occurred: {e}')
                                oracle_conn.rollback()
                                raise
                else:
                    logging.info("No data returned from MS SQL Server.")

    extract_sql_query() >> execute()

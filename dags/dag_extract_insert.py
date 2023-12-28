import cx_Oracle
from contextlib import closing
import pandas as pd
from airflow.hooks.base import BaseHook 
import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
        dag_id='dat_oracle_to_postgres',
        start_date=pendulum.datetime(2023, 12, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    @task(task_id='task1')
    def select_from_oracle(**kwargs):
        rdb = BaseHook.get_connection('conn-db-oracle-custom')
        data=[]
        ti = kwargs['ti']

        ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                                    user=rdb.login,
                                    password=rdb.password,
                                    encoding="UTF-8")
        
        ora_cursor = ora_con.cursor()    
        ora_cursor.execute("SELECT * FROM test")
 

        for row in ora_cursor.fetchall():
            processed_row = []
            for cell in row:
                if isinstance(cell, cx_Oracle.LOB):
                    processed_row.append(cell.read())
                else:
                    processed_row.append(cell)
            data.append(processed_row)   

        columns = [desc[0] for desc in ora_cursor.description]
        ora_cursor.close()
        ora_con.close()

        ti.xcom_push(key="columns", value=columns)
        ti.xcom_push(key="rows", value=data)    
    

    @task(task_id='task2')
    def matchingModel(**kwargs): ## postgres와 oracleDB 열 이름 다를 때 서로 매칭 해 주기. 
        ti = kwargs['ti']
        #oracle_data = ti.xcom_pull(task_ids='task1')
        model = ti.xcom_pull(key="columns", task_ids = 'task1')
        oracle_row = ti.xcom_pull(key="rows", task_ids = 'task1')      

        switch_dict = {
        
            'test_01': 'test_a',
            'test_02': 'test_b',
            'test_03': 'test_c'
                    }
    
        colname_list=[switch_dict.get(item.lower(), item.lower()) for item in model]
        return {

            "sql":','.join(colname_list),
            "oracleRow":oracle_row,
            "column_count":len(colname_list)

        }

 


    @task(task_id='task3')
    def exec_insert(**kwargs): #100개 단위로 batch작업
        # 데이터베이스 연결 생성
        ti = kwargs['ti']  

        pg_hook = PostgresHook('conn-db-postgres-custom') 
        conn = pg_hook.get_conn() 

        oracle_data = ti.xcom_pull(task_ids='task2')       
        insertIntoCol = oracle_data['sql']
        rowFromOracle = oracle_data['oracleRow'] 
        columnCount = oracle_data['column_count'] 
        
        try: 
            
          
            placeholders = ["%s",] * int(columnCount)        
            tuples = [tuple(item) for item in rowFromOracle]

            with closing(conn.cursor()) as cur:                
                
                sql = f"INSERT INTO test ({insertIntoCol}) VALUES ({','.join(placeholders)})"
                cur.executemany(sql, tuples) #executemany() 는 psycopg2에서 제공해주는 라이브러리로 bulk upload가능            
                
        finally:
            conn.commit()
            conn.close()   
        return tuples
     

    
    select_from_oracle() >> matchingModel() >> exec_insert()
import cx_Oracle
from contextlib import closing
import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook 
import pendulum
from airflow.decorators import task
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def select_from_oracle():
    rdb = BaseHook.get_connection('conn-db-oracle-custom')
    data=[]

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
    
    return columns,data #컬럼들, data들

@task
def select_from_postgresColumns_toInsert(conn, tbl_name):
    try:
        cursor = conn.cursor()
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = 'public'
        """
        cursor.execute(query, (tbl_name,))
        result = cursor.fetchall()
        column_names = [column[0] for column in result]
        columns_string = ', '.join(column_names)
        cursor.close()

        return {
            'column_name': columns_string, ## insert into 할 컬럼 명
            'column_count': len(result) ## insert into 할 컬럼 개수
        }

    except psycopg2.Error as e:
        print("Database error: ", e)
        return None

def matchingModel(model): ## postgres와 oracleDB 열 이름 다를 때 서로 매칭 해 주기. 
    switch_dict = {
     
        'test_01': 'test_a',
        'test_02': 'test_b',
        'test_03': 'test_c'
                }
   

    return ','.join([switch_dict.get(item.lower(), item.lower()) for item in model])

def generate_insert_sql(table, target_fields = None, replace=False, **kwargs) -> str:
    # insert문 
    placeholders = [
            "%s",
        ] * len('test') ##예시

    if target_fields:
        target_fields = ", ".join(target_fields)
        target_fields = f"({target_fields})"
    else:
        target_fields = ""

    if not replace:
        sql = "INSERT INTO "
    else:
        sql = "REPLACE INTO "
    sql += f"{table} {target_fields} VALUES ({','.join(placeholders)})"
    return sql


@task
def exec_insert(insertIntoCol, rowFromOracle, commit_every=1000): #100개 단위로 batch작업
    # 데이터베이스 연결 생성
    pg_hook = PostgresHook('conn-db-postgres-custom')


    conn = psycopg2.connect(dbname=pg_hook.schema, user=pg_hook.login, password=pg_hook.password, host=pg_hook.extra_dejson.get("host"), port=pg_hook.port)
    postgreTable = 'test'
    
    try: 
        with closing(conn):
            # 여기서 연결을 관리
            conn.commit()
            postgreData = select_from_postgresColumns_toInsert(conn, postgreTable) # 오라클 결과를 집어넣을 postgres 테이블 명 
   
            placeholders = ["%s",] * int((postgreData['column_count']))        
            tuples = [tuple(item) for item in rowFromOracle]
   
            with closing(conn.cursor()) as cur:                
                
                sql = f"INSERT INTO test ({insertIntoCol}) VALUES ({','.join(placeholders)})"
                cur.executemany(sql, tuples) #executemany() 는 psycopg2에서 제공해주는 라이브러리로 bulk upload가능
            conn.commit()
            
    except psycopg2.Error as e:
        print("Database error: ", e)
    finally:
        # 연결 닫기
        conn.close()



    

with DAG(
        dag_id='dat_oracle_to_postgres',
        start_date=pendulum.datetime(2023, 12, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:

    OracleColumn, OracleRow = select_from_oracle()
    insertSQL = matchingModel(OracleColumn);
    exec_insert(insertSQL,OracleRow)
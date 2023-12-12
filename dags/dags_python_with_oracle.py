
from airflow import DAG
import pendulum
from airflow.decorators import task
#from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.hooks.base import BaseHook 

from airflow.providers.postgres.hooks.postgres import PostgresHook
import cx_Oracle


@task
def get_data_from_oracle():
  
    rdb = BaseHook.get_connection('conn-db-oracle-custom')

  
    ora_con = cx_Oracle.connect(dsn=rdb.extra_dejson.get("dsn"),
                                user=rdb.login,
                                password=rdb.password,
                                encoding="UTF-8")

    ora_cursor = ora_con.cursor()    

    #oracle_hook = OracleHook('conn-db-oracle-custom')
    #data = oracle_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}") ## transaction  자재로 쓸 수 있음. 오라클로부터 Extract
    #return data.to_dict()
    
  
    query = "SELECT * FROM test2"
    ora_cursor.execute(query)    
    rows = ora_cursor.fetchall()
    columns = [col[0] for col in ora_cursor.description]
    result_as_dict = [dict(zip(columns, row)) for row in rows]                    

    ora_cursor.close()
    ora_con.close()

    return result_as_dict

@task
def insert_data_into_postgres(data):
    import psycopg2
    from psycopg2 import sql
    pg_hook = PostgresHook('conn-db-postgres-custom')
    #pg_hook.insert_rows(table="test" ,rows=data)
    post_conn = psycopg2.connect(dbname=pg_hook.database, user=pg_hook.login, password=pg_hook.password, host=pg_hook.host, port=pg_hook.port)
    cursor = post_conn.cursor()

    table_name = "test"
    columns_to_select = ["TEST_ID", "TEST_01", "TEST_02"]
    condition = "1 = 1"  # 선택적인 WHERE 절

    query = sql.SQL("SELECT {} FROM {}").format(
        sql.SQL(", ").join(map(sql.Identifier, columns_to_select)),
        sql.Identifier(table_name)
    )


    if condition:
        query += sql.SQL(" WHERE {}").format(sql.SQL(condition))

    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()

    return result

    
with DAG(
        dag_id='dags_python_with_oracle',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:

    data = get_data_from_oracle()
    insert_data_into_postgres(data)


from airflow import DAG
import pendulum
from airflow.decorators import task
#from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.hooks.base import BaseHook 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.operators.oracle import OracleOperator

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'pepega',
}

table_name="test"
@task
def get_data_from_oracle():
     oracle_task = OracleOperator(
        task_id='oracle',
        oracle_conn_id='oracle_test',
        sql='SELECT * FROM DUAL',
        dag=dag,
     )
    
    #oracle_hook = OracleHook('conn-db-oracle-custom')
    #data = oracle_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}") ## transaction  자재로 쓸 수 있음. 오라클로부터 Extract
    #return data.to_dict()
    
   

@task
def insert_data_into_postgres(data):
    pg_hook = PostgresHook('conn-db-postgres-custom')
    pg_hook.insert_rows(table="test" ,rows=data)
    
with DAG(
        dag_id='dags_python_with_oracle',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False,
        tags=['pepega']
) as dag:
    #data = get_data_from_oracle()
    #insert_data_into_postgres(data)
    get_data_from_oracle

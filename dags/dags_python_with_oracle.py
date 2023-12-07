'''
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

table_name="test"
@task
def get_data_from_oracle():
    oracle_hook = OracleHook('conn-db-oracle-custom')
    data = oracle_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}") ## transaction  자재로 쓸 수 있음. 오라클로부터 Extract
    return data.to_dict()

@task
def insert_data_into_postgres(data):
    pg_hook = PostgresHook('conn-db-postgres-custom')
    pg_hook.insert_rows(table="test" ,rows=data)
    
with DAG(
        dag_id='dags_python_with_oracle',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    data = get_data_from_oracle()
    insert_data_into_postgres(data)
'''

from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator

with DAG(
        dag_id='dags_python_with_postgres',
        start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
        schedule=None,
        catchup=False
) as dag:
    def insrt_postgres(postgres_conn_id, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        from contextlib import closing
        
        postgres_hook = PostgresHook(postgres_conn_id)
        with closing(postgres_hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                dag_id = kwargs.get('ti').dag_id
                task_id = kwargs.get('ti').task_id
                run_id = kwargs.get('ti').run_id
                msg = 'hook insrt 수행'
                sql = 'insert into test values (%s,%s,%s,%s);'
                cursor.execute(sql, (dag_id, task_id, run_id, msg))
                conn.commit()

    insrt_postgres_with_hook = PythonOperator(
        task_id='insrt_postgres_with_hook',
        python_callable=insrt_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom'}
    )
    insrt_postgres_with_hook
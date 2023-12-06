from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from contextlib import closing
import pendulum

# DAG 생성
dag = DAG(
    dag_id='dags_python_with_postgres',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    schedule_interval=None,
    catchup=False,
)

# DAGBag Import Timeout 설정
dag.set_dagbag_import_timeout(120)  # 설정할 timeout 값 (초 단위)

# PostgreSQL에 삽입하는 함수
def insrt_postgres(postgres_conn_id, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id)
    with closing(postgres_hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            dag_id = kwargs.get('ti').dag_id
            task_id = kwargs.get('ti').task_id
            run_id = kwargs.get('ti').run_id
            msg = 'hook insrt 수행'
            sql = 'insert into test values (%s, %s, %s, %s);'
            cursor.execute(sql, (dag_id, task_id, run_id, msg))
            conn.commit()

# PythonOperator로 PostgreSQL 함수 실행
insrt_postgres_with_hook = PythonOperator(
    task_id='insrt_postgres_with_hook',
    python_callable=insrt_postgres,
    op_kwargs={'postgres_conn_id': 'conn-db-postgres-custom'},
    dag=dag,  # DAG 객체를 PythonOperator에 전달
)


from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import cx_Oracle


table_name="test"
@task
def get_data_from_oracle():
    # Oracle 연결 정보 설정
    oracle_conn_str = "username/password@hostname:port/service_name" ##
    connection = cx_Oracle.connect(oracle_conn_str)

    # SQL 쿼리 작성
    sql_query = "SELECT * FROM your_oracle_table"

    # 쿼리 실행
    cursor = connection.cursor()
    cursor.execute(sql_query)

    # 데이터 가져오기
    data = cursor.fetchall()
    data.to_dict()
    print(data)

    # 연결 닫기
    cursor.close()
    connection.close()

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

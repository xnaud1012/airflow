
from airflow import DAG
import pendulum
from airflow.decorators import task
#from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import cx_Oracle
table_name="test"
@task
def get_data_from_oracle():
    db_user = "your_username"
    db_password = "your_password"
    db_host = "your_host"
    db_port = "your_port"
    db_service = "your_service_name"

    dsn = cx_Oracle.makedsn(db_host, db_port, service_name=db_service)
    connection_string = f"{db_user}/{db_password}@{dsn}"
    connection = cx_Oracle.connect(connection_string)
    cursor = connection.cursor()

    #oracle_hook = OracleHook('conn-db-oracle-custom')
    #data = oracle_hook.get_pandas_df(sql=f"SELECT * FROM {table_name}") ## transaction  자재로 쓸 수 있음. 오라클로부터 Extract
    #return data.to_dict()
    
    try:
        query = "SELECT * FROM test"
        cursor.execute(query)    
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        result_as_dict = [dict(zip(columns, row)) for row in rows]
                     
        for row in rows:
            print(row)    

    finally:
        cursor.close()
        connection.close()

    return result_as_dict

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

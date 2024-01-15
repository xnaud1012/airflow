from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow import DAG

import pendulum

with DAG(
    dag_id='dag_trigger_rule_example',
    start_date=pendulum.datetime(2024,1,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    bash_upstream1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )
    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')
    
    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상처리')

    @task(task_id='python_downstream_1',trigger_rule='all_done') # trigger_rule종류 정해져있음(문서 참조) / all_done : 모두 실행되기만 하면 후행 태스크 실행 
    def python_downstream_1():
        print('정상처리')

    [bash_upstream1, python_upstream_1(), python_upstream_2()]>>python_downstream_1()
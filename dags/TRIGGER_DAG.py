from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
    dag_id='TRIGGER_DAG',
    start_date=pendulum.datetime(2024,1,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    start_task=BashOperator(
        task_id='start_task',
        bash_command='echo "start"'
    )
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='dag_python_operator', #필수값, 트리거링 하고자 하는 후행 DAG
        trigger_run_id=None, 
        #trigger_run_id = DAG의 수행방식(Schedlue,  manual, Backfill)과 시간을 유일하게 식별해주는 키 (uid같은 느낌)
        #ex 스케줄에 의해 실행될 경우 scheduled_{{data_interval_Start}}값 가짐.
        execution_date='{{data_interval_Start}}',
        reset_dag_run=True,
        wait_for_completion=True, # T2과 선행 DAG, T3가 후행 DAG이고 T2가 C라는 제 3의 DAG를 트리거링 할 때, 
                                  # C까지 success 되어야 T3이 실행되는 구조 (True)
        poke_interval=60, # wait_for_completion과 이어짐
        failed_states=None,
        allowed_states=['success'] # T2 태스크가 success로 표시 되기 위해서 C의 결과가 뭐라고 표시되어야 하는지. 
    )
    start_task >> trigger_dag_task
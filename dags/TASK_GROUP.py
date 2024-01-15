from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import pendulum

with DAG(
    dag_id='TRIGGER_RULE',
    start_date=pendulum.datetime(2024,1,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    
    def inner_func(**kwargs):
        msg=kwargs.get('msg') or ''
        print(msg)
    @task.group(group_id='first_group')
    def group_1():

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('Task Group내 첫 번째 task')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫번째 taskgroup 내 두 번째 task'}
        )

        inner_func1()>>inner_function2 # task 데코레이터로 작성, pythonoperator로 작성

    with TaskGroup(group_id='second_group',tooltip='두번째 그룹') as group_2:
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두번 째 taskgroup 내 첫 번째 task입니다')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'두 번째 Taskgroup내 두 번 째 taskgroup이다.'}
        )
        inner_func()>>inner_function2
    group_1()>>group_2  # 항상 테스크 데코레이터 
from airflow import DAG
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    dag_id='dags_branch_python_operator_example',
    start_date=pendulum.datetime(2024,1,1, tz='Asia/Seoul'), 
    schedule='0 1 * * *',
    catchup=False
) as dag:
    def select_random(): #return 값은 task 이름(객체)이어야 함.
        import random

        item_lst = ['A','B','C']
        selected_item = random.choice(item_lst)
        if selected_item == 'A':
            return 'task_a'
        elif selected_item in ['B','C']:
            return ['task_b','task_c']
    
    python_branch_task = BranchPythonOperator(
        task_id='python_branch_task',
        python_callable=select_random # retun 받은 task 명 
    )
    
    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected':'A'}
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected':'B'}
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected':'C'}
    )

    python_branch_task >> [task_a, task_b, task_c] # python_branch_task 라는 태스크가 선행 태스크, 뒤의 []안에 있는 태스크들이 후행 태스크 
from airflow import DAG
import pendulum
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_base_branch_operator',
    start_date=pendulum.datetime(2024,1,1, tz='Asia/Seoul'),
    schedule=None,
    catchup=False
) as dag:
    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):   # 오버라이딩 
                                            #'choose_branch' 함수명 워딩을 바꾸면 안됨. 꼭 이 이름 그대로 써야 함. /파라미터명 context도 바꾸면 안됨. 
            import random
            print(context)
            
            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']

    
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task') #객체 생성 

    
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

    custom_branch_operator >> [task_a, task_b, task_c]
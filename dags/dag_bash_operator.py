from __future__ import annotations
import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_bash_operator", # DAG이름 # 되도록이면 파이썬 파일명과일치 시키는 게 좋음 
    schedule="0 0 * * *", #크론 스케줄 : 분,시,일,월,요일 / 주기마다 작업.
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False, # 2023-1-1 이후 작업을 돌리지 않은 누락된 구간을 소급 돌릴 지 여부 /false 가 기본값 
    #dagrun_timeout=datetime.timedelta(minutes=60), # 60분 이상 돌면실패처리
  #  tags=["example", "example2"], #없어도 됨. 태그 
  #  params={"example_key": "example_value"}, #공통적으로 넘겨줄 파라미터가 있는 경우 작성
) as dag:
    run_this_last = EmptyOperator(
        task_id="run_this_last",
    )

    # [START howto_operator_bash]
    bash_T1 = BashOperator(
        task_id="bash_T1",
        bash_command="echo Task1", # 실행 할 bash문 
    )
    # [END howto_operator_bash]

    bash_T2 = BashOperator(
        task_id="bash_T2",
        bash_command="echo $HOSTNAME", #실행 할 bash문 
    )
    bash_T1>>bash_T2
  
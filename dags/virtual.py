from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator
import requests
import os
from airflow.models import Variable
from myairflow.send_notification import send_noti


def f_vpython(dis):
    from myairflow.send_notification import send_noti
    send_noti(f"time : {dis} : Jacob vpython")
    #ds = data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')
    #message= f"{dag.dag_id} {task.task_id} {ds} OK / Jacob"
    #send_noti(message)

def f_python(data_interval_start,**kwargs):
    from myairflow.send_notification import send_noti
    send_noti(f"time : {data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')}  : Jacob python")
    #ds = data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')
    #message= f"{dag.dag_id} {task.task_id} {ds} OK / Jacob"
    #send_noti(message)         

# Directed Acyclic Graph
with DAG(
    "virtual",
    schedule="@hourly",
    # start_date=datetime(2025, 3, 10)
    start_date=pendulum.datetime(2025, 3, 12, tz="Asia/Seoul"),
    default_args={"depend_on_past":False},
    max_active_runs=1
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    t_vpython = PythonVirtualenvOperator(
        task_id="t_vpython",
        python_callable=f_vpython,
        requirements=["git+https://github.com/Jacob-53/myairflow.git@0.1.0"],
        op_args=["{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y%m%d%H')}}"]
        #trigger_rule=TriggerRule.ONE_FAILED 
    )
    t_python = PythonOperator(
        task_id="t_python",
        python_callable=f_python,
        #trigger_rule=TriggerRule.ONE_FAILED 
    )
    #sleep = BashOperator(task_id="sleep",
    #             bash_command="sleep 5")
    start >> [t_python,t_vpython] >> end
    
    # start >> b1
    # b1 >> [b2_1, b2_2]
    # [b2_1, b2_2] >> end
    
    # start >> b1 >> b2_1
    # b1 >> b2_2
    # [b2_1, b2_2] >> end
if __name__ == "__main__":
    dag.test()
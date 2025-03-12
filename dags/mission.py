from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.operators.python import PythonVirtualenvOperator


def abc():
    pass

with DAG(
    "myetl",
    #schedule=timedelta(days=1),
    #schedule="* * * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025,3,12, tz="Asia/Seoul"),
    catchup=True,
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    make_data = BashOperator(
                task_id="mk_data",
                bash_command=
                "bash /home/jacob/airflow/make_data.sh /home/jacob/data/{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"
                )
    
    load_data = PythonVirtualenvOperator(task_id="load_data",
                                            python_callable= abc,
                                            requirements=["git+https://github.com/Jacob-53/myairflow.git@0.1.0"],)
    agg_data = PythonVirtualenvOperator(task_id="agg_data",
                                            python_callable= abc,
                                            requirements=["git+https://github.com/Jacob-53/myairflow.git@0.1.0"],)
     
    start >> make_data >> load_data >> agg_data >> end
if __name__ == "__main__":
    dag.test()
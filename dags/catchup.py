from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

#DAG (Directed Acyclic Graph)
with DAG(
    "catchup",
    #schedule=timedelta(days=1),
    #schedule="* * * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025,3,10, tz="Asia/Seoul"),
    catchup=False
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    start >> end
    
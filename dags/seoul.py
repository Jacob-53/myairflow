from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

#DAG (Directed Acyclic Graph)
with DAG(
    "seoul",
    #schedule=timedelta(days=1),
    #schedule="* * * * * *",
    schedule="@hourly",
    start_date=pendulum.datetime(2025,3,10, tz="Asia/Seoul")
    ) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    b1=BashOperator(
        task_id="b_1",
        bash_command="""
        echo "date ======================> 'date'",
        echo "data_interval_start =======> {{data_interval_start.in_tz("Asia/Seoul")}}",
        echo "data_interval_end =========> {{data_interval_end.in_tz("Asia/Seoul")}}",
        echo "ds=========================>{{ds.replace("-", "/")}}",
        echo "ts=========================>{{ts}}",
        echo "execution_date ============>{{execution_date}}",
        echo "prev_execution_date =======>{{prev_execution_date}}",
        echo "next_execution_date =======>{{next_execution_date}}",
        echo "test=======================>{{data_interval_start.in_tz('Asia/Seoul').strftime('%Y/%m/%d/%H')}}"      
        """)
    b2_1=BashOperator(task_id="b_2_1", bash_command="echo 2_1")
    b2_2=BashOperator(task_id="b_2_2", bash_command="echo 2_2")
    
    #OR start >>b1>>[b2_1,b2_2]>> end
    
    #OR
    #start >> b1
    #b1>>b2_1
    #b1>>b2_2
    #b2_1>>end
    #b2_2>>end
 
    #OR    
    # start >> b1
    #b1>>[b2_1,b2_2]
    #[b2_1,b2_2]>> end
    
    #OR
    start >> b1>> b2_1
    b1>>b2_2
    [b2_1,b2_2]>> end
    
    
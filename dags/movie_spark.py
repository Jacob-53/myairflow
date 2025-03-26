from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "movie_spark"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=5,
    description="movie spark sbumit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 2),
    catchup=True,
    tags=["spark", "sbumit", "movie"],
) as dag:
    SPARK_HOME="/Users/jacob/app/spark-3.5.1-bin-hadoop3"
    SCRIPT_BASE="/Users/jacob/app/spark-3.5.1-bin-hadoop3/bin"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    spark_submit = BashOperator(
        task_id='submit', 
        bash_command='$SPARK_HOME/bin/spark-submit $SCRIPT_BASE/movie_meta.py {{ ds_nodash }}',
        env={"SPARK_HOME": SPARK_HOME, "SCRIPT_BASE": SCRIPT_BASE}
        )
    start >> spark_submit >> end
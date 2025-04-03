from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DAG_ID = "wiki_save"

with DAG(
    DAG_ID,
    default_args={
        "depends_on_past": True,
        "retries": 1,
        "retry_delay": timedelta(seconds=3),
    },
    max_active_runs=1,
    max_active_tasks=1,
    description="wiki spark gcp submit",
    schedule="10 10 * * *",
    start_date=datetime(2024, 3, 1),
    end_date=datetime(2024, 3, 31),
    catchup=True,
    tags=["spark", "sh", "wiki"],
) as dag:

    start = EmptyOperator(task_id="start")

    run_bash = BashOperator(
        task_id='run_bash',
        bash_command="""
        ssh -i ~/.ssh/jacob_gcp_key jacob8753@34.64.92.116 \
        "/home/jacob8753/code/test/run.sh {{ ds }} /home/jacob8753/code/test/test_save_parquet.py"
        """,
    )
    check_success = BashOperator(
        task_id='check_success',
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS="/Users/jacob/keys/abiding-ascent-455400-u6-c8e90511db0d.json"
        FILE_INFO=$(gsutil ls -l gs://jacob-wiki-bucket/wiki/test/parquet/_SUCCESS | grep -v TOTAL)
        FILE_DATE=$(echo $FILE_INFO | awk '{print $2}' | cut -d'T' -f1)

        if [ "$FILE_DATE" = "{{ ds }}" ]; then
        echo "✅ _SUCCESS 파일이 오늘 생성됨 → 성공!"
        exit 0
        else
        echo "❌ _SUCCESS 파일이 오늘 생성된 게 아님 → 실패 처리"
        exit 1
        fi
        """,
    )

    start >> run_bash >> check_success
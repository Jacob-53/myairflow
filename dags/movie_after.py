from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
        BranchPythonOperator, 
        PythonVirtualenvOperator,
)
from airflow.utils.trigger_rule import TriggerRule
import os
from airflow.sensors.filesystem import FileSensor

DAG_ID = "movie_after"
start_date = datetime(2024, 1, 1)
with DAG(
    'movie_after',
    default_args={
        'depends_on_past': True,
        'retries': 1,
        'retry_delay': timedelta(seconds=10),
        'start_date': start_date,
    },
    max_active_runs=1,
    max_active_tasks=1,
    description='movie after',
    schedule="10 10 * * *",
    start_date = start_date,
    end_date = datetime(2024, 1, 11),
    catchup=True,
    tags=['api', 'movie','sensor'],
) as dag:
    REQUIREMENTS = ["git+https://github.com/Jacob-53/movie.git@0.6.6"]
    BASE_DIR = f"/home/jacob/data/{DAG_ID}"
    start = EmptyOperator(task_id='start')
    end= EmptyOperator(task_id='end')
    
    check_done  = FileSensor(task_id="check.done",
                             filepath="/home/jacob/data/movies/done/dailyboxoffice/{{ds_nodash}}/_DONE",
                             fs_conn_id="fs_after_movie",
                             poke_interval=180,  # 3분마다 체크
                             timeout=3600,  # 1시간 후 타임아웃
                             mode="reschedule",  # 리소스를 점유하지 않고 절약하는 방식
                             )
    
    def fn_gen_meta(ds_nodash,base_path,start_date,**kwargs):
        import json
        from movie.api.call import gen_meta
        genmeta = gen_meta(ds_nodash,base_path,start_date)
        print("::group::gen_meta to save...")
        print("ds_nodash--->" + ds_nodash)
        print("gen_meta--->" + genmeta)
        print("::endgroup::")
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))

    gen_meta = PythonVirtualenvOperator(
        task_id="gen.meta",
        python_callable=fn_gen_meta,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs = {"base_path": BASE_DIR,"start_date":start_date.strftime('%Y%m%d')}
        
    )

    def fn_gen_movie(base_path, ds_nodash , **kwargs):
        from movie.api.call import gen_movie
        import json
        genmovie = gen_movie(base_path,ds_nodash, partitions=[])
        print("::group::gen_movie to save...")
        print("ds_nodash--->" + ds_nodash)
        print("gen_meta--->" + genmovie)
        print("::endgroup::")
        print(json.dumps(kwargs, indent=4, ensure_ascii=False))
        print(f"base_path: {base_path}/dailyboxoffice")

    gen_movie = PythonVirtualenvOperator(
        task_id="gen.movie",
        python_callable=fn_gen_movie,
        requirements=REQUIREMENTS,
        system_site_packages=False,
        op_kwargs={"base_path": BASE_DIR}
    )

    make_done = BashOperator(
        task_id="make.done",
        bash_command="""
        DONE_BASE=$BASE_DIR/done
        echo $DONE_BASE
        mkdir -p $DONE_BASE/{{ ds_nodash }}
        touch $DONE_BASE/{{ ds_nodash }}/_DONE
        """,
        env={'BASE_DIR':BASE_DIR},
        append_env = True
    )

start >> check_done >> gen_meta >> end
#  >> gen_movie >> make_done
if __name__ == "__main__":
    dag.test()

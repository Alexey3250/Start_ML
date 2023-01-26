from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'a_vedenina_task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dinamic DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    time = '{{ ts }}'
    run_id = '{{ run_id }}'
    for i in range(5):
        t1 = BashOperator(
            task_id=f"print_ts_i_run_id_{i}",
            bash_command=f"echo {time} {run_id}",
            dag=dag
        )

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta


def print_task_number(task_number, ts, run_id):
    print(f"task number is: {task_number}")
    print(ts)
    print(run_id)


with DAG(
    'step_7_gagarin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG_for_step_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 12),
    catchup=False,
    tags=['step_3_gagarin']
) as dag:
    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id='step' + str(i),
                bash_command="echo $NUMBER",
                env={"NUMBER: i"}
            )
        else:
            task = PythonOperator(
                task_id='step' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )

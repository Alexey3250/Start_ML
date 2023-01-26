from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'a-feofanova_lesson_11_hw_step_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'BashOperator_with_variable',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 12, 14),
    catchup = False,
    tags = ['Try to use variable'],
) as dag:

    for task_number in range(10):
        t1 = BashOperator(
            task_id = f'bash_cycle_number_{task_number}',
            retries = 4,
            bash_command = "echo $NUMBER",
            env = {'NUMBER': task_number},
        )

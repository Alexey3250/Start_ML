from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'j-jancharskaja_7',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My sixth DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['sixth']
) as dag:

    tasks = dict()

    def print_num(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts, run_id)    

    for i in range(10, 30):
        tasks[('t' + str(i))] = PythonOperator(
            task_id = 'command_' + str(i),
            python_callable = print_num,
            op_kwargs = {'task_number': i}
        )

tasks['t10'] >> tasks['t11'] >> tasks['t12'] >> tasks['t13'] >> tasks['t14'] >> tasks['t15'] >> tasks['t16'] >> tasks['t17'] >> tasks['t18'] >> tasks['t19'] >> tasks['t20'] >> tasks['t21'] >> tasks['t22'] >> tasks['t23'] >> tasks['t24'] >> tasks['t25'] >> tasks['t26'] >> tasks['t27'] >> tasks['t28'] >> tasks['t29']
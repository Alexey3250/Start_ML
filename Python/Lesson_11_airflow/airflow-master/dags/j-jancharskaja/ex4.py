from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'j-jancharskaja_4',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My third DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['third']
) as dag:

    tasks = dict()

    for i in range(10):
        tasks[('t' + str(i))] = BashOperator(
            task_id = 'command_' + str(i),
            bash_command = f'echo {i}',
            doc_md = dedent(
                """\
            # __*Task Documentation*__
            Prints value of the variable `i`.

            """            
        ))

    def print_num(task_number):
        print('task number is: {task_number}')

    for i in range(10, 30):
        tasks[('t' + str(i))] = PythonOperator(
            task_id = 'command_' + str(i),
            python_callable = print_num,
            op_kwargs = {'task_number': i},
            doc_md = dedent(
                """\
            # __*Task Documentation*__
            Prints value of the variable `i`.

            """            
        ))

tasks['t0'] >> tasks['t1'] >> tasks['t2'] >> tasks['t3'] >> tasks['t4'] >> tasks['t5'] >> tasks['t6'] >> tasks['t7'] >> tasks['t8'] >> tasks['t9'] >> tasks['t10'] >> tasks['t11'] >> tasks['t12'] >> tasks['t13'] >> tasks['t14'] >> tasks['t15'] >> tasks['t16'] >> tasks['t17'] >> tasks['t18'] >> tasks['t19'] >> tasks['t20'] >> tasks['t21'] >> tasks['t22'] >> tasks['t23'] >> tasks['t24'] >> tasks['t25'] >> tasks['t26'] >> tasks['t27'] >> tasks['t28'] >> tasks['t29']
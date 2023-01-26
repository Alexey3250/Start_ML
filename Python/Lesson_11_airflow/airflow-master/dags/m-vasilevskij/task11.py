from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta
from textwrap import dedent
from psycopg2.extras import RealDictCursor
import psycopg2


def print_text(ts, run_id, **kwargs):
    print(f"task number is: {kwargs['task_number']}")
    print(f"ts parameter: {ts}")
    print(f"run_id parameter': {run_id}")

def push_xcom(ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = 'xcom test'
    )

def pull_xcom(ti):
    ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'push_data'
    )

def return_xcom(ti):
    return 'Airflow tracks everything'

def get_xcom(ti):
    ti.xcom_pull(
        key = 'return_value',
        task_ids = 'return_data'
    )

def get_variable():
    print(Variable.get('is_startml'))


def get_connection():
    creds = BaseHook.get_connection('startml_feed')
    with psycopg2.connect(
    f"postgresql://{creds.login}:{creds.password}"
    f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor(cursor_factory = RealDictCursor) as cursor:
            cursor.execute(
                '''
                SELECT user_id, COUNT(post_id) as "count"
                FROM feed_action
                GROUP BY user_id
                '''
            )
            print(cursor.fetchall())
            return cursor.fetchall()

with DAG(

    'hw11_m-vasilevskij',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description = 'DAG for lesson 11',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 12, 14),
    catchup = False

) as dag:

#     for i in range(0, 30):
#         if i < 10:
#             task = BashOperator(
#                 task_id = f"task_{i}",
#                 depends_on_past = False,
#                 retries = 3,
#                 bash_command = "echo $NUMBER",
#                 env = {'NUMBER': i}
#             )
#         else:
#             task1 = PythonOperator(
#                 task_id = f"task_{i}",
#                 depends_on_past = False,
#                 retries = 3,
#                 python_callable = print_text,
#                 op_kwargs = {'task_number': i}
#             )

    # task2 = PythonOperator(
    #     task_id = 'return_data',
    #     python_callable = return_xcom,
    # )

    # task3 = PythonOperator(
    #     task_id = 'pull_data',
    #     python_callable = get_xcom,
    # )

    # task4 = PythonOperator(
    #     task_id = 'print_variable',
    #     python_callable = get_variable, 
    # )

    task5 = PythonOperator(
        task_id = 'connection',
        python_callable = get_connection,
    )

    task5

    # task2 >> task3

    # task >> task1
                #     SELECT user_id, COUNT(post_id) as "count"
                # FROM feed_action
                # GROUP BY user_id
                # ORDER BY COUNT(post_id) DESC
                # LIMIT 1
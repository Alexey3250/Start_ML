# from airflow import DAG
# from airflow.models import Variable
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.bash import BashOperator
# from datetime import datetime, timedelta


# with DAG(
#     'kant_hw13',
#     default_args={
#         'depends_on_past': False,
#         'email': ['airflow@example.com'],
#         'email_on_failure': False,
#         'email_on_retry': False,
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5), 
#     },
#     description='exersize_13',
#     schedule_interval=timedelta(days=1),
#     start_date=datetime(2022, 3, 10),
#     catchup=False,
#     tags='kant_hw13',
# ) as dag:

#     start = DummyOperator(
#         task_id = 'start'
#     )

#     def choose_branch():
#         is_startml = Variable.get("is_startml")
#         print(f'is_startml: {is_startml}')
#         print(type(is_startml))
#         if is_startml:
#             return "startml_desc"
#         else:
#             return "not_startml_desc"

#     choose_course = BranchPythonOperator(
#         task_id = 'choose_branch',
#         python_callable = choose_branch,
#     )

#     startml =  BashOperator(
#         task_id = 'startml_desc',
#         bash_command = 'echo "StartML is a starter course for ambitious people"',
#     )

#     not_startml =  BashOperator(
#         task_id = 'not_startml_desc',
#         bash_command = 'echo "Not a startML course, sorry"',
#     )

#     end = DummyOperator(
#         task_id = 'end'
#     )

#     start >> choose_course >> [startml, not_startml] >> end
    #                           -->    startml   --
    #                          |                   |
    # start -> choose_branch --                    |--> end
    #                          |                   | 
    #                           -->  not_startml --


from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2
from airflow.models import Variable

with DAG(
    'kant_hw13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='kant_hw13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 11),
    catchup=False,
    tags='kant_hw13',
) as dag:
    
    def get_condition():
        if Variable.get('is_startml') == 'True':
            return "startml_desc"
        return "not_startml_desc"

    def startml_desc():
            print("StartML is a starter course for ambitious people")


    def not_startml_desc():
            print("Not a startML course, sorry")

    t1 = BranchPythonOperator(
            task_id='check_course',
            python_callable=get_condition,
            trigger_rule='one_success'
    )

    t2 = PythonOperator(
            task_id='startml_desc',
            python_callable=startml_desc,
    )

    t3 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=not_startml_desc,
    )

    t1 >> [t2, t3] 
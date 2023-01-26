#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import airflow
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    'a-bugaj-14-task-4',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
 description='TASK_11_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):

        t1 = BashOperator(
        task_id='echo_for_' + str(i),
        env={'NUMBER' : str(i)},
        bash_command="echo $NUMBER"
        )

    def print_task_number(ts, run_id, **kwargs):

        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for i in range(20):

        t2 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number' : i}
        )
        
        t1.doc_md = dedent(
    """\
    # Documentation
    ###`Look! This is code`
    ###*This one is italic*
    ###**And this one is bold**
    """
    )

    t1 >> t2


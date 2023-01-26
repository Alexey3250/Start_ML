#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable


def print_var():
    is_startml = Variable.get('is_startml')
    print(is_startml)

    
with DAG(
    'a-bugaj-14-task-12',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
 description='TASK_11_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:   
    
    t = PythonOperator(
        task_id = 'print_var',
        python_callable=print_var,
    )


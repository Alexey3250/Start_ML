#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator


def choose_task():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    if is_startml=='True': return 'startml_desc'
    else: return 'not_startml_desc'


def yes_task():
    print('StartML is a starter course for ambitious people')


def no_task():
    print('Not a startML course, sorry')

    
with DAG(
    'a-bugaj-14-task-13',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
 description='TASK_11_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:   
    
    
    branch = BranchPythonOperator(
        task_id='choose_task',
        python_callable=choose_task,
    )
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=yes_task,
    )
    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=no_task,
    )
    t0 = DummyOperator(
        task_id='start',
    )
    t999 = DummyOperator(
        task_id = 'finish',
    )

    t0 >> branch >> [t1, t2] >> t999


# In[ ]:





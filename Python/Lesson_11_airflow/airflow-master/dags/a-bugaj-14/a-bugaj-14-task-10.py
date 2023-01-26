#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import time
import requests
import json
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG


def xcom_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )
    return 'Airflow tracks everything'


def xcom_pull(ti):
    my_xcom_test = ti.xcom_pull(
        key='return_value',
        task_ids='get_xcom_test'
    )
    print('Test xcom getting: ', my_xcom_test)

with DAG(
    'a-bugaj-14-task-10',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
 description='TASK_11_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:


    t1 = PythonOperator(
        task_id = 'get_xcom_test',
        python_callable = xcom_push
    )

    t2 = PythonOperator(
        task_id = 'pull_xcom_test',
        python_callable = xcom_pull
    )

    t1 >> t2


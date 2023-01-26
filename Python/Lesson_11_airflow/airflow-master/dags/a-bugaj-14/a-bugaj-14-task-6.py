#!/usr/bin/env python
# coding: utf-8

# In[7]:


import airflow
import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('a-bugaj-14-task-6',
        default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description='TASK_11_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

     for i in range(10):
        t1=BashOperator(
            task_id="a"+str(i),            
            bash_command="echo $NUMBER",
            env={"NUMBER":str(i)}
            )

        t1
    


# In[ ]:





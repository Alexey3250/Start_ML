#!/usr/bin/env python
# coding: utf-8

# In[1]:


import airflow
from datetime import timedelta, datetime
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(
    'a-bugaj-14-task-5',

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

    templated_code = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """)
    a = BashOperator(
        task_id='command',
        bash_command=templated_code)


# In[ ]:





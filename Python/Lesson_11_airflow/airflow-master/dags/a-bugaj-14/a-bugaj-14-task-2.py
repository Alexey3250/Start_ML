#!/usr/bin/env python
# coding: utf-8

# In[2]:


from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('a-bugaj-14-task-2',
        default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    description='TASK_11_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    t1 = BashOperator(
        task_id='print_date',  
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
    return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
    task_id='print_the_context',  
    python_callable=print_context, 
)
    
    t1 >> t2
    


# In[ ]:





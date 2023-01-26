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
    'a-bugaj-14-task-11',

    default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
 description='TASK_11_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    
    def get_user_put_max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                     SELECT user_id, COUNT(user_id)
                     FROM feed_action
                     WHERE action = 'like'
                     GROUP BY user_id
                     ORDER BY COUNT(user_id) DESC
                     LIMIT 1 
                     '''
                )
                result = cursor.fetchone()
                return result

    t1 = PythonOperator(
        task_id='get_user_put_max_likes',
        python_callable=get_user_put_max_likes,
    )


# In[ ]:





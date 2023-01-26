#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def determing_course():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def not_startml_desc():
    print("Not a startML course, sorry")
    
def startml_desc():
    print("StartML is a starter course for ambitious people")
    
with DAG(
    'k-tjan_exercise_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise_12 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['exercise_12'],
) as dag:

    t1 = DummyOperator(task_id='before_branching')
    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determing_course,
        )
    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
        )
    t4 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
        )
    t5 = DummyOperator(task_id='after_branching')

t1 >> t2 >> [t3, t4] >> t5
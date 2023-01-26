#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'k-tjan_exercise_3',    
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise_3 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['exercise_3'],
) as dag:
    def print_stuff(stuff):
        print(stuff)
    
    for i in range(10):
        t1 = BashOperator(
            task_id='This is BashOperator Task#' + str(i),  # id, будет отображаться в интерфейсе
            python_callable=print_stuff,
            op_kwargs={'stuff': f"Iteration# {i}"}
        )
    
    def print_task_id(task_number):
        print("task number is: {task_number}")
    
    for i in range(20):
        t2 = PythonOperator(
            task_id='This is PythonOperator Task#' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_task_id,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i}
        )


    t1 >> t2
    
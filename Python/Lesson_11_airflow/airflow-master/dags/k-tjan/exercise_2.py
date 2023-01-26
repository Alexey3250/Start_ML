#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 17 20:55:13 2022

@author: keyreeltian
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'k-tjan_hw_1',
    # Параметры по умолчанию для тасок
    
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_1 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['hw_1'],
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )
    
    def print_ds(ds):
        print(ds)
        return f'Just printed {ds}'
    
    t2 = PythonOperator(
        task_id='print_ds',  # нужен task_id, как и всем операторам
        python_callable=print_ds,  # свойственен только для PythonOperator - передаем саму функцию
    )


    t1 >> t2
    
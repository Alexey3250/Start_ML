#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# import requests
# import json


def x_push(ti, **kwargs):
    k = kwargs['k']
    v = kwargs['v']
    print(f"key={k} and value={v}")
    ti.xcom_push(
        key=k,
        value=v
    )


def x_pull(ti, **kwargs):
    k = kwargs['k']
    print(f"key={k}")
    res = ti.xcom_pull(
        key=k,
        task_ids='task_push'
    )
    print(f"Pulled value is {res}")


with DAG(
    'k-tjan_exercise_8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise_8 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['exercise_8'],
) as dag:

    t1 = PythonOperator(
        task_id='task_push',
        python_callable=x_push,
        op_kwargs={'k': "sample_xcom_key", 'v': "xcom test"}
        )

    t2 = PythonOperator(
        task_id='task_pull',
        python_callable=x_pull,
        op_kwargs={'k': "sample_xcom_key"}
        )

    t1 >> t2

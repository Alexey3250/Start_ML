from airflow import DAG
from textwrap import dedent
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'el-pogarskaja-14_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description = 'A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        ) as dag:
    #function that returns a string
    def return_str():
        return "Airflow tracks everything"

    t1 = PythonOperator(
            task_id = 'from_func',
            python_callable=return_str,
            )
    t1.doc_md = dedent(
            """
            t1 simply gets the string from the function **return_str**.
            """
            )
    #functiom that pulls string from XCom
    def pull_xcom(ti):
        ti.xcom_pull(
                key='return_value',
                task_ids='from_func'
                )
    t2 = PythonOperator(
            task_id='from_XCom',
            python_callable=pull_xcom,
            )
    t2.doc_md = dedent(
            """
            t2 shows that `return` in another function automatically pushed the returned meaning in XCom. 
            #t2 gets the meaning from XCom.
            """
            )
    t1 >> t2


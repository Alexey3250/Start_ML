from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def x_com_push():
    """
    test XCom push
    """
    return "Airflow tracks everything"

def x_com_pull (ti):
    """
    testing XCom pull
    """
    testing_pull = ti.xcom_pull(
        key='return_value',
        task_ids='some_task_pull_ids'
    )
    print(testing_pull)
    return "end x_com_pull"

with DAG(
    # название
'hw_10_d-otkaljuk',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='Py and Bush operation',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 10, 31),
catchup=False,
tags=['hw_10_d_otkaljuk'],
) as dag:
    opr_get_push = PythonOperator(
        task_id = 'some_task_pull_ids',
        python_callable= x_com_push,
    )
    opr_get_pull = PythonOperator(
        task_id = 'Look_analyze_end_task_ids',
        python_callable = x_com_pull,
    )

    opr_get_push >> opr_get_pull
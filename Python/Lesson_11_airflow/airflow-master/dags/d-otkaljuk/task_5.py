from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent
# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
    # название
'hw_5_d-otkaljuk',
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
start_date=datetime(2022, 10, 30),
catchup=False,
tags=['hw_5_d_otkaljuk'],
) as dag:# Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение

    templated_command = dedent(
            """
            {% for i in range(5) %}
                 echo  "{{ ts }}"
                 echo "{{ run_id }}"
            {% endfor %}
            """
        )
    t1 = BashOperator(
        task_id="t1_pwd",
        bash_command= templated_command
    )
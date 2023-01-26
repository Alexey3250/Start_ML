'''
Создайте новый DAG, состоящий из одного BashOperator.
Этот оператор должен  использовать шаблонизированную команду следующего вида:
"Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id".
Здесь ts и run_id - это шаблонные переменные
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'k-shilin-15_task4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'Task 3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id _}}"
    {% endfor %}
    """
    )

    t = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )
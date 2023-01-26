from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

'''
Создайте новый DAG, состоящий из одного BashOperator. 
Этот оператор должен  использовать шаблонизированную команду следующего вида: 
"Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id". 
Здесь ts и run_id - это шаблонные переменные 
'''

default_args={
    'depends_on_past': False,
    'email':  ['airflow@example.com'],
    'email_on_falure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_5_ponomareva',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva']
) as dag:

    templated_command = dedent(
    '''
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id}}"
    {% endfor %}
    ''')

    t1 = BashOperator(
        task_id='HW_5_Bash',
        bash_command=templated_command,
    )

    t1
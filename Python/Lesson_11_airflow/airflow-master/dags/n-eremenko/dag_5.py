from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)  # timedelta из пакета datetime
}
with DAG('hw_2_n-eremenko',
         default_args = default_args,
         description ='hw_2_',
         schedule_interval=timedelta(days=1),
         start_date = datetime(2022,9, 9),
         catchup = False,
         tags=['hw_2_n-eremenko']) as dag:

    def print_for_20(task_number):
        print(f'task number is: {task_number}')


    bash_commands =dedent('''
    {% for i in range(5) %}
        echo {{ ts }}
        echo {{ run_id }}
    {% endfor %}''')

    for i in range(10):
        t1 = BashOperator(task_id='bash_for_echo_'+str(i),
                          bash_command= bash_commands)

    t1.doc_md = dedent('''
    ##Task docunebtation
    `{% for i in range(10) %}
        echo{{ i }}
    {% endfor %}`
    _cursive_
    **jir**
    ''')

    t1
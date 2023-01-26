from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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
    {% for i in range(10) %}
        echo{{ i }}
    {% endfor %}''')
    for i in range(10):
        t1 = BashOperator(task_id='bash_for_echo_'+str(i),
                          bash_command= f'echo {i}')
    for i in range(20):
        task = PythonOperator(task_id='print_for_20_python_'+str(i),
                            python_callable=print_for_20,
                            op_kwargs= {'task_number':int(i)})

    t1.doc_md = dedent('''
    ##Task docunebtation
    `{% for i in range(10) %}
        echo{{ i }}
    {% endfor %}`
    _cursive_
    **jir**
    ''')
    task.doc_md = dedent('''
        ##Task docunebtation
        `print(f\'task number is: {task_number}\')`
        _cursive_
        **jir**
        ''')
    t1 >> task
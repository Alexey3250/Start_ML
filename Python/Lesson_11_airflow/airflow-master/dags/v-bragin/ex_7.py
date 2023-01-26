from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

with DAG(
    'bragin_ex_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'DAG ex_7',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 9, 9),
    catchup = False,
    tags=['example'],
) as dag:

    def print_context(task_number, ts, run_id):
        print(f'task_number is {task_number}')
        print(ts)
        print(run_id)


    for i in range(20):
        run_this = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )
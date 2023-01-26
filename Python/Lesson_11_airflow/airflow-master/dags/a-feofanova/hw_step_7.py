from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'a-feofanova_lesson_11_hw_step_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'PythonOperator_with_kwargs',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022,12,13),
    catchup = False,
    tags = ['trying to make PythonOperator with kwargs'],
) as dag:

# добавляю в функцию прием аргументов ts, run_id
# печатаю эти значения
    def print_context(task_number, ts, run_id):
        return f'task number is: {task_number}'
        print(ts),
        print(run_id)

# в PythonOperator добавляю kwargs
# со значением task_number
    for task_n in range(20):
        task = PythonOperator(
            task_id=f'python_cycle_number_{task_n}',
            python_callable=print_context,
            op_kwargs={'task_number': task_n,
                       'kwargs': task_n},
            )

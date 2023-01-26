from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Пишу функцию, по которой достаю Variable
# делаю импорт Variable
# is_startml - название моей Varieble
# передаю is_startml Variable.get с ключом = 'is_startml'
# печатаю переменную
def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    print (is_startml)

with DAG(
    'a-feofanova_lesson_11_hw_step_12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'DAG prints Variable',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2022, 12, 14),
        catchup = False,
        tags = ['DAG with PythonOperator, prints Variable'],
) as dag:

# таска, которая напечатает Variable
    task = PythonOperator(
        task_id = 'print_variable',
        python_callable = get_variable,
    )

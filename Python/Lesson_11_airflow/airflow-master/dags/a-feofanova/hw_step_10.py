from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'a-feofanova_lesson_11_hw_step_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = 'Implicit XCom DAG',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 12, 13),
    catchup = False,
    tags = ['DAG has 2 python operators'],
) as dag:

# функция, которая бдет возвращать строку
# все возвращенные (return) значения записываются
# в XCom по ключу return_value
    def return_str():
        return 'Airflow tracks everything'

# функция, которая получит строку из таска t1
# через XCom
# ключ return_value
# task_ids - из какого таска брать значение
    def get_str(ti):
        ti.xcom_pull(
        key = 'return_value',
        task_ids = 'return_string'
        )

# таск 1, который возвращает строку
# с помощью функции return_str
    t1 = PythonOperator(
        task_id = 'return_string',
        python_callable = return_str
    )

# таск 2, который берет строку из t1
# через XCom
    t2 = PythonOperator(
        task_id = 'get_string',
        python_callable = get_str
    )

t1 >> t2

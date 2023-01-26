from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

# пишу функцию, которая будет класть
# значение 'xcom test' по ключу sample_xcom_key в XCom
def push_test_data (ti):
    ti.xcom_push(
        key = 'sample_xcom_key',
        value = 'xcom test'
    )

# пишу функцию, которая будет доставать значение
# 'xcom test' и печатать его
# в task_ids передаю id таска t1
def take_and_print_test_data(ti):
    ti.xcom_pull(
        key = 'sample_xcom_key',
        task_ids = 'push_test_data'
    )

with DAG(
    'a-feofanova_lesson_11_hw_step_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'DAG_with_two_python_operators and xcom',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2022,12,12),
        catchup=False,
        tags = ['xcom DAG']
) as dag:

# таск, который будет класть значение 'xcom test'
# в XCom (с помощью функции push_test_data)
    t1 = PythonOperator(
        task_id = 'push_test_data',
        python_callable = push_test_data,
    )

# таск, который достает значение 'xcom test'
# и печатает его (функция take_and_print_test_data)
    t2 = PythonOperator(
        task_id = 'take_and_print_test_data',
        python_callable = take_and_print_test_data,
    )

t1 >> t2

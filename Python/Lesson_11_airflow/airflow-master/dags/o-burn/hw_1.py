'''
Напишите DAG, который будет содержать BashOperator и PythonOperator. В функции PythonOperator примите аргумент ds и
распечатайте его. Можете распечатать дополнительно любое другое сообщение.
В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow.
Результат может оказаться неожиданным, не пугайтесь - Airflow может запускать ваши задачи на разных машинах или
контейнерах с разными настройками и путями по умолчанию.
Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator.
'''

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_11_1_allburn',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='HW_1_burn',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['o-burn example'],
) as dag:

    t1 = BashOperator(
        task_id='show_pwd',
        bash_command='pwd'
    )


    def func_ds(ds, **kwargs):
        print (ds)
        print (kwargs)
        return 'just want to see wtf'

    t2 = PythonOperator(
        task_id = 'show_ds',
        python_callable = func_ds
    )

    t1 >> t2


from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

with DAG(
    'task7_evseev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task3 DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 9, 14),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['hw_7_m_evseev_12_task7'],
) as dag:

    def print_func(task_number, ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(kwargs['task_number'])
        return print("task number is: {task_number}")

    # Генерируем таски в цикле - так тоже можно
    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        t1 = PythonOperator(
            task_id='print_current_num_' + str(i + 10),  # в id можно делать все, что разрешают строки в python
            python_callable=print_func,
            op_kwargs={'task_number': int(i) + 10},
        )

    t1


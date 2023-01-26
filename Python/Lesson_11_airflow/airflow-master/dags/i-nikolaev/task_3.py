from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
with DAG(
    # Название
    'i_Nikolaev_task_3',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
    def get_num(ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(f"task number is: {kwargs['task_number']}")

    # Генерируем таски в цикле - так тоже можно
    for i in range(10):
        taskBash = BashOperator(
            task_id='command_Bash_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command = "echo $NUMBER",
            env = {"NUMBER": str(i)}
        )
    taskBash.doc_md = dedent(
        """
    #### Task Documentation
    You **can** document your *task* using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    # rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )
    for i in range(20):
        taskPython = PythonOperator(
            task_id='com_Python_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=get_num,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )
        # настраиваем зависимости между задачами
    taskBash >> taskPython
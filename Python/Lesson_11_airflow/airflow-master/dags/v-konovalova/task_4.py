"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'konovalova_4',
    # Параметры по умолчанию для тасок
    default_args={ # аргументы по умолчанию
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
    
           
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    for i in range(10):
        t1 = BashOperator(
            task_id='task_3' + str(i),
            depends_on_past=False,  # id, будет отображаться в интерфейсе
            bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )

    def print_context(task_number):
        """Пример PythonOperator"""
        # Через синтаксис **kwargs можно получить словарь
        # с настройками Airflow. Значения оттуда могут пригодиться.
        # Пока нам не нужно
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(f'task number is: {task_number}')
    
    t1.doc_md = dedent(
        '''\
    #### Task Documentation
    '''
    )

    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number_' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': int(i)}
        )

    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3
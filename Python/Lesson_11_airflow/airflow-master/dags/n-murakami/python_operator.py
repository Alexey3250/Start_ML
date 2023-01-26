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

# Объявление при помощи контекстного менеджера
with DAG(
    # Имя дага
    'python_operator',
    # Параметры по умолчанию для всех тасок
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
    # логическая дата
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['py_oper'],
) as dag:
    # Объявляем задачи внутри контекстного менеджера
    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    # Выполняет некоторую команду в консоли Linux (команда date показывает текущую дату)
    t1 = BashOperator(
        task_id='print_date',  # id, будет отображаться в интерфейсе
        bash_command='date',  # какую bash команду выполнить в этом таске
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,  # переопределили настройку из DAG
        bash_command='sleep 5', # заставляет терминал заснуть на 5 сек
        retries=3,  # тоже переопределили retries (было 1)
    )
    # прописываем документацию для тасков
    # для избежания проблем с экранированием при использовании markdow
    # нужно весь текст заключить в тройные кавычки и оборнуть функцией dedent
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку

    dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
    dag.doc_md = """
    This is a documentation placed anywhere
    """  # а можно явно написать
    # формат ds: 2021-12-25 -  текущая логическая дата (заполняется на стороне airflow)
    # echo распечатка в консоль
    # %{}% все что лежит внутри принадлежит синтаксису шаблонизатора
    # {{}} подстановка переменной
    # macros.ds_add(ds, 7) - функция jinja - берет ds и добавляет 7 дней
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )  # поддерживается шаблонизация через Jinja
    # Jinja - библиотека, позволяющая написать шаблоны для программ
    # используется, когда имеется несколько однотипных программ, в которых нельзя использовать переменные
    # например, сложный фреймворк, принимающий на вход файлы конфигурации, написанный на другом языке
    # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating
    # airflow принимает шаблон через аргумент bash_command,
    # потом прогоняет через шаблонизатор, подставляет переменные, выполняет функции
    # и только потом отдает Linux серверу на выполнение
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
        # Через синтаксис **kwargs можно получить словарь
        # с настройками Airflow. Значения оттуда могут пригодиться.
        # Пока нам не нужно
        # Airflow будет передавать в функцию огромное кол-во аргументов
        # **kwargs означает, что функция может принемать сколько угодно аргументов
        print(kwargs)
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        # ds - текущая логическая дата в формате строки
        print(ds)
        return 'Whatever you return gets printed in the logs'


    t4= PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    def my_sleeping_function(random_base):
        """Заснуть на random_base секунд"""
        import time
        time.sleep(random_base)

    # Генерируем таски в цикле - так тоже можно
    for i in range(5):
        # Каждый таск будет спать некое количество секунд
        task = PythonOperator(
            task_id='sleep_for_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_sleeping_function,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'random_base': float(i) / 10},
        )
        # настраиваем зависимости между задачами
        # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        t4 >> task

    # А вот так в Airflow указывается последовательность задач
    t1 >> [t2, t3] >> t4
    # будет выглядеть вот так
    #      -> t2 ->
    #  t1 |       | -> t4 -> [...]
    #      -> t3 ->
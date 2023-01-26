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
    'hw_11_2_d-solovev-9',
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
    description='SDV dag for task 11',
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
#     t1 = BashOperator(
#         task_id='print_date',  # id, будет отображаться в интерфейсе
#         bash_command='date',  # какую bash команду выполнить в этом таске
#     )

    get_pwd = BashOperator(
        task_id='getpwd',
        depends_on_past=False,  # переопределили настройку из DAG
        bash_command='pwd',
        retries=3,  # тоже переопределили retries (было 1)
    )
#     t1.doc_md = dedent(
#         """\
#     #### Task Documentation
#     You can document your task using the attributes `doc_md` (markdown),
#     `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
#     rendered in the UI's Task Instance Details page.
#     ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

#     """
#     )  # dedent - это особенность Airflow, в него нужно оборачивать всю доку

#     dag.doc_md = __doc__  # Можно забрать докстрингу из начала файла вот так
#     dag.doc_md = """
#     This is a documentation placed anywhere
#     """  # а можно явно написать
#     # формат ds: 2021-12-25
#     templated_command = dedent(
#         """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#     {% endfor %}
#     """
#     )  # поддерживается шаблонизация через Jinja
#     # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating

#     t3 = BashOperator(
#         task_id='templated',
#         depends_on_past=False,
#         bash_command=templated_command,
#     )
    
#     templated_command = dedent(
#         """
#         {% for i in range(5) %}
#             echo "{{ dag_run.logical_date | ts }}"
#         {% endfor %}
#         echo "{{ run_id }}"
#         """
#     )  # поддерживается шаблонизация через Jinja
#     # https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#concepts-jinja-templating

#     jin_task = BashOperator(
#         task_id='templated',
#         depends_on_past=False,
#         bash_command=templated_command,
#     )
    
#     def print_context(ds, **kwargs):
#         """Пример PythonOperator"""
#         # Через синтаксис **kwargs можно получить словарь
#         # с настройками Airflow. Значения оттуда могут пригодиться.
#         # Пока нам не нужно
# #         print(kwargs)
#         # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
#         print(ds)
#         return 'Whatever you return gets printed in the logs'
    
        

# 
    def print_context(task_number, ts, run_id, **kwargs):
        """ PythonOperator - печать номера таска"""
        print(f"task number is: {kwargs.get('task_number')}")
        print(f"task time is: {kwargs.get('ts')}")
        print(f"run_id is: {kwargs.get('run_id')}")
#         t_num = kwargs.get('task_number')
        return print(f"task number is: {kwargs.get('task_number')}") # 'Whatever you return gets printed in the logs'
    
#     python_get_ds = PythonOperator(
#         task_id='print_the_context',  # нужен task_id, как и всем операторам
#         python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
#     )
                     
    # Генерируем таски в цикле - так тоже можно
    for i in range(31):
        # Каждый таск будет спать некое количество секунд
        if i < 10 :
            task = BashOperator(
                    task_id='echo_bash' + str(i),
                    depends_on_past=False,
                    bash_command=f'echo {i}', #Задание 2
#                     bash_command='echo $NUMBER',
#                     env={"NUMBER": str(i)}
  
            )
        else:
            task = PythonOperator(
                task_id='print_task_number'+ str(i),  # в id можно делать все, что разрешают строки в python
                
                # передаем в аргумент с названием random_base значение float(i) / 10
                op_kwargs={'task_number': i,
                           'ts' : '{{ ts }}',
                           'run_id' : '{{run_id}}'
                          },
                provide_context=True,
                python_callable=print_context,
            )
        
        dag.doc_md = __doc__             
        task.doc_md = dedent(
            """\
        #### Task Documentation
        You can document your task using the attributes `doc_md` (markdown),
        `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
        rendered in the UI's Task Instance Details page.
        Or you can use `code` like `def func(): return True`.
        *some italic* text
        **some bold** text
        `` some monospace`` text
        ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

        """
        ) 

        # настраиваем зависимости между задачами
        # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        get_pwd >> task

    # А вот так в Airflow указывается последовательность задач
#     get_pwd >> jin_task
    get_pwd #>> python_get_ds
#     python_get_ds >> jin_task
    # будет выглядеть вот так
    #      -> t2
    #  t1 | 
    #      -> t3
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


def post_info_to_xcom(ti):
    """ Функция публиикации информации, где
    key= ключ, по которому в функции get мы будем запрашивать значения
    value= значения, которые мы получим при запросе из xcom в логах
    """
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def get_info_from_xcom(ti):
    """ Функция получения информации, где
    key= ключ из функции публикации
    task_ids = ids задачи, которую нужно будет указать task_id, после вызова функции в PythonOperator в
    post_info_to_xcom в переменной post_info_xcom или иной переменной публикации
    """
    get_info = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids="Get_info_from_xcom"
    )
    print(get_info)  # печать запроса в логах
    return "Everything is good" # можем кинуть в логи строку, что понять, где конец логов
# Создаем DAG. DAG - это инструкция, как выполнять процесс обработки оператора (таска)
with DAG(
'hw_9_v2_e-poljakov-13', # название DAG
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
},  description='hw_3',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),  # Как часто запускать DAG
    start_date=datetime(2022, 1, 1), # С какой даты начать запускать DAG. Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    catchup=False,  # Запустить за старые даты относительно сегодня,
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    tags=['poljakov-13'],  # теги, способ помечать даги
) as dag:   # Операторы - это кирпичики DAG, они являются звеньями в графе. В них прописывается команды на исполнение
   # функция записи инфы в xcom
    post_info_xcom = PythonOperator(
        task_id="Get_info_from_xcom",  # ids из def Get_info_from_xcom. Это делаеться для того, что xcom
        # понимал из какой задачи брать инфу для публикации
        python_callable=post_info_to_xcom  # вызов def post_info_to_xcom - функция публикации
    )
   # функция получения инфы с xcom
    get_info_xcom = PythonOperator(
        task_id="Look_info_from_task_Get_info_from_xcom",  # просто ids
        python_callable=get_info_from_xcom  # вызов def get_info_from_xcom
    )

    post_info_xcom >> get_info_xcom

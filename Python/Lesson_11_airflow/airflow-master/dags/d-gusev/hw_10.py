from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def push_xcom_test():
    return "Airflow tracks everything"

def pull_xcom_test(ti):
    return_value = ti.xcom_pull(
        key="return_value",
        task_ids="push_xcom_test"
    )
    print(return_value)

with DAG(
        'hw_10_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_10']
) as dag:

    push_value = PythonOperator(
        task_id='push_xcom_test',
        python_callable=push_xcom_test,
    )

    push_value.doc_md = dedent(
        """
        ## Создайте новый DAG, содержащий два `PythonOperator`.
        Первый оператор должен вызвать функцию,
        **возвращающую** строку `"Airflow tracks everything"`.
        """
    )

    return_value = PythonOperator(
        task_id='pull_xcom_test',
        python_callable=pull_xcom_test,
    )

    return_value.doc_md = dedent(
        """
        ## Второй оператор должен получить эту строку через XCom.
        *Вспомните по лекции, какой должен быть ключ.*
        Настройте правильно последовательность операторов.
        """
    )

    push_value >> return_value
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator



with DAG(
        'hw_9_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_9']
) as dag:

    def push_xcom_test(ti):
        ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
        )
    
    def pull_xcom_test(ti):
        value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push_xcom_test"
        )
        print(value)

    push_value = PythonOperator(
        task_id='push_xcom_test',
        python_callable=push_xcom_test,
    )

    push_value.doc_md = dedent(
        """
        ##Сделайте новый DAG, содержащий два Python оператора.
        Первый `PythonOperator` должен класть в **XCom**
        значение `"xcom test"` по ключу `"sample_xcom_key"`. 
        """
    )

    pull_value = PythonOperator(
        task_id='pull_xcom_test',
        python_callable=pull_xcom_test,
    )

    pull_value.doc_md = dedent(
        """
        ## Второй `PythonOperator` должен доставать это значение
        и печатать его.
        Настройте правильно последовательность операторов.
        """
    )

    push_value >> pull_value
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_3_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['hw_3'],
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
                echo "{{ ts }}"
            {% endfor %}
            echo "{{ run_id }}"

        """
    )

    t1 = BashOperator(
        task_id='11_5',
        depends_on_past=False,
        bash_command=templated_command
    )

    t1

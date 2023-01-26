from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent


with DAG(
    'print_context_t5',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:

    for i in range(5):
        task = dedent(
            """
            {% for i in range(5) %}
                echo "{{ ds }}"
                echo "{{ run_id }}"
            {% endfor %}
            """)
        task
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'a-feofanova_lesson_11_hw_step_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG with BahOperator which uses a template',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 14),
    catchup=False,
    tags=['DAG with templates'],
) as dag:

    # буду писать BashOperator, который использует шаблонизированную команду
    # ниже шаблонизация через Jinja
    # Для каждого i от 0 до 5 (не вкл) распечатать ts,
    # затем распечатать run_id
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ts}}"
            echo "{{run_id}}"
        {% endfor %}
        """
    )

    # Пишу таску - BashOperator с использованием шаблона
    task = BashOperator(
        task_id = 'templated_task',
        depends_on_past = False,
        retries = 3,
        bash_command = templated_command,
    )

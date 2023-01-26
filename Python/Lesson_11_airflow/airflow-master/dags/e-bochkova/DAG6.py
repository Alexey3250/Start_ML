from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


with DAG(
    'hw_6_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework3'],
) as dag:
    t1 = BashOperator(
        task_id="task_0",
        env={"NUMBER": 0},
        bash_command="echo $NUMBER",
    )
    for i in range(9):
        task = BashOperator(
            task_id=f"task_{i+1}",
            env={"NUMBER": i+1},
            bash_command="echo $NUMBER",
        )
        t1 >> task
        t1 = task
from airflow import DAG

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

with DAG(
        'kolomiets_11_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="Lerning DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 12, 21),
        catchup=False,
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'e-kolomiets_10_echos_{i}',
            env=({'NUMBER': i}),
            bash_command="echo $NUMBER"
        )

    t1

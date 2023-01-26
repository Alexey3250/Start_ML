from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


with DAG(
    'ignatov_8',
    default_args={
      'depends_on_past': False,
      'email': ['ignik18@gmail.com'],
      'email_on_failure': False,
      'email_on_retry': False,
      'retries': 1,
      'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2022, 9, 12)
) as dag:
    def print_number(ts, run_id, **kwargs):
        kwargs['task_number'] = i
        print(ts, run_id)

    for i in range(1, 31):
        if i < 11:
            bash_task = BashOperator(
                task_id=f'{i}',
                bash_command="echo $NUMBER",
                env={'NUMBER': str(i)}
            )
        else:
            python_task = PythonOperator(
                task_id=f'{i}',
                python_callable=print_number,
                op_kwargs={'task_number': i}
            )

    bash_task.doc_md = dedent(
        """\
        ### Task Documentation
        _This task are to test my_ __skills__
        `python_task = PythonOperator(
                task_id=f'{i}',
                python_callable=print_number,
                op_kwargs={'task_number': i}
            )`
        """
    )
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG('task_1') as dag:
    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='python',
        python_callable=print_ds
    )

    t1 = BashOperator(
        task_id='bash',
        bash_command='pwd'
    )

    t1 >> t2

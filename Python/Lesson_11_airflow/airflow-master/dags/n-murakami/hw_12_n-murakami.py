from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_var():
    from airflow.models import Variable
    var = Variable.get("is_startml")
    print(var)

with DAG(
        'hw_12_n-murakami',
        default_args={
          'depends_on_past': False,
          'retries': 3,
          'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_12_n-murakami']
) as dag:
  t= PythonOperator(
    task_id="print_var",
    python_callable=get_var
  )


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
import psycopg2


def find_user(conn_id):
    creds = BaseHook.get_connection(conn_id)
    with psycopg2.connect(
  f"postgresql://{creds.login}:{creds.password}"
  f"@{creds.host}:{creds.port}/{creds.schema}"
) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT user_id, count(*)
from feed_action
where action = 'like'
group by user_id
order by count(*) DESC
limit 1;
""")        
            columns = list(cursor.description)
            results = cursor.fetchone()
            dict_results = {}
            for col, val in zip(columns, results):
                dict_results[col.name] = val
            return dict_results

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_9_o_bulaeva_10',
    start_date=datetime(2022, 7, 29),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'find_user',
        python_callable=find_user,
        op_kwargs = {'conn_id':'startml_feed'}
    )

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'task_11_v_demareva',
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Lesson 11 (Task 11)',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 21),
        catchup=False,
        tags=["task_11"]
) as dag:

    def set_variable():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT user_id, COUNT(action) as count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1
                    """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id="t11_python_1",
        python_callable=set_variable,
    )

    t1
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
        'hw_5_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_5']
) as dag:
    print_template = dedent(
        """ 
        {% for i in range(5) %}
            echo "{{ ts }}" 
        {% endfor %} 
        echo "{{ run_id }}" 
        """
    )

    template_engine = BashOperator(
        task_id='print_template',
        bash_command=print_template
    )

    template_engine.doc_md = dedent(
        """
        #Создайте новый **DAG**, состоящий из одного `BashOperator`.
        #  Этот оператор должен  использовать *шаблонизированную* команду следующего вида:
        "Для каждого `i` в диапазоне от 0 до 5 не включительно распечатать значение `ts`
        и затем распечатать значение `run_id`". Здесь `ts` и `run_id` -
        это шаблонные переменные (вспомните, как в лекции подставляли шаблонные переменные).
        """
    )

    template_engine
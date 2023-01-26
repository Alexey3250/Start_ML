from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

# достаю Variable под названием is_startml
def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    print (is_startml)

# пишу функцию для бранчинга
# если is_startml=True, перезодим в таск
# с task_id='startml_desc'
# если is_startml=False, перезодим в таск
# с task_id='not_startml_desc'
def decide_which_path():
    if bool(is_startml):
        return 'startml_desc'
    if not bool(is_startml):
        return 'not_startml_desc'

# пишу функцию для t2 (startml_desc)
def startml_course():
    print('StartML is a starter course for ambitious people')

# пишу функцию для t3 (not_startml_desc)
def not_startml_course():
    print('Not a startML course, sorry')

with DAG(
    'a-feofanova_lesson_11_hw_step_13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'DAG with branching',
        schedule_interval = timedelta(days = 1),
        start_date = datetime(2022, 12, 14),
        catchup = False,
) as dag:

# пишу таск t1 для того, чтобы достать Variable 'is_startml'
    t1 = PythonOperator(
        task_id = 'print_variable',
        python_callable = get_variable,
    )

# пишу DummyOperator
# он ничего не делает, но перед ветвлением
# задает красивую стартову точку на графе
    dummy_task_before = DummyOperator(
        task_id = 'determine_course',
    )

# для ветвления описываю BranchPythonOperator
# он опрнднлит, в какое ответвление пойдет DAG
    branch_task = BranchPythonOperator(
        task_id = 'path_choosing',
        python_callable = decide_which_path,
    )

# Описываю t2(startml_desc)
    t2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_course,
    )

# Описываю t3(not_startml_desc)
    t3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_course,
    )

# также делаю DummyOperator в конце dag
# чтобы сделать крсивое завершение
    dummy_task_after = DummyOperator(
        task_id = 'after_branching'
    )

# определяю порядок выполнения тасок
dummy_task_before >> branch_task >> [t2, t3] >> dummy_task_after

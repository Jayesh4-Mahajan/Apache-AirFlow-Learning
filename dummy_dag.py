from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args={
    'owner': 'Jayesh Mahajan',
    'start_date': days_ago(0),
    'email': ['jayeshmahajan@somewhere.py'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='My Dummy Dag',
    schedule_interval=timedelta(minutes=1),
)

task1 = BashOperator(
    task_id='task1',
    bash_command='sleep 1',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command='sleep 2',
    dag=dag,
)

task3 = BashOperator(
    task_id='task3',
    bash_command='sleep 3',
    dag=dag,
)

task1 >> task2 >> task3

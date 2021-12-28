# import the libraries

from datetime import timedelta
from airflow import DAG #The DAG object; Need this to instantiate a DAG
from airflow.operators.bash_operator import BashOperator # Operators; need this to write tasks
from airflow.utils.dates import days_ago # For scheduling

# defining DAG arguments
# We can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'Jayesh Mahajan',
    'start_date': days_ago(0),
    'email': ['jayeshmahajan@somewhere.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

dag = DAG(
    'dag-sample',
    default_args=default_args,
    description='DAG Bash Example',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# First Task
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > extracted-data.txt',
    dag=dag,
)

# Second Task
transform_and_load = BashOperator(
    task_id='transform',
    bash_command='tr ":" "," < extracted-data.txt > transformed-data.csv',
    dag=dag,
)

# task pipeline

extract >> transform_and_load

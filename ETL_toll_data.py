# Import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# Task 1.1 - Define DAG arguments
default_args = {
    "owner": "Ben Greene",
    "start_date": days_ago(0),
    "email": ["ben.greene@email.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


# Task 1.2 - Define the DAG
dag = DAG(
    dag_id="ETL_toll_data",
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description="Apache Airflow Final Assignment"
)


# define the task named 'extract_transform_load'
extract_transform_load = BashOperator(
    task_id='extract_transform_load',
    bash_command="./home/project/airflow/dags/extract_transform_load.sh",
    dag=dag
)


# task pipeline
extract_transform_load

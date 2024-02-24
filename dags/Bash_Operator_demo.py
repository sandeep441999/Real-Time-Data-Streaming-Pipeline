from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Sandeep',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="my_first_dag_v5",
    default_args=default_args,
    description="This is a Bash Operator Dag",
    start_date=datetime(2024,1,1, 2),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id="First_task",
        bash_command="echo Hello World, This is Sandeep Pabbu"
    )

    task2 = BashOperator(
        task_id="Second_task",
        bash_command="echo Hello World, This is Second Task"
    )

    task3 = BashOperator(
        task_id="Third_task",
        bash_command="echo Hello World, This is Third Task after task 1 along with task2"
    )

    #
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    task1 >> [task2, task3]
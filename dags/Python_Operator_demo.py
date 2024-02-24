from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Sandeep',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


def welcome():
    print("Welcome. Buddy!")


def details(ti):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    interest = ti.xcom_pull(task_ids="get_interest", key="interest")
    print(f"My name is {first_name} {last_name} and I am love playing {interest}")


def get_name(ti):
    ti.xcom_push(key="first_name", value="Sandeep")
    ti.xcom_push(key="last_name", value="Pabbu")


#    return "Sandeep"

def get_interest(ti):
    ti.xcom_push(key="interest", value="Cricket")


with DAG(
        dag_id="my_first_python_operator_dag_v7",
        default_args=default_args,
        description="This is a Python Operator Dag",
        start_date=datetime(2024, 2, 11, 2),
        schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id="get_name",
        python_callable=get_name
    )

    task2 = PythonOperator(
        task_id="details",
        python_callable=details,
        # op_kwargs={'interest': "Cricket"}
    )

    task3 = PythonOperator(
        task_id="get_interest",
        python_callable=get_interest
    )

    #
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # task1 >> task2
    # task1 >> task3

    # task1 >> [task2, task3]

    [task1, task3] >> task2

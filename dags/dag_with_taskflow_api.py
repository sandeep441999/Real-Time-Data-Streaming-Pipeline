from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import dag, task

default_args = {
    'owner': 'Sandeep',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id="my_taskflow_API_DAG_v2",
    default_args=default_args,
    start_date=datetime(2024,2,11, 2),
    schedule_interval='@daily')
def hello_world():

    @task(multiple_outputs=True)
    def get_name():
        return {
            "first_name": "Sandeep",
            "last_name": "Pabbu"
        }

    @task()
    def get_interest():
        return "Cricket"

    @task()
    def welcome(first_name, last_name, interest):
        print(f"Hello World, This is {first_name} {last_name}, I love playing {interest}.")

    name_dict=get_name()
    interest=get_interest()
    welcome(first_name=name_dict["first_name"], last_name=name_dict["last_name"], interest=interest)


greet = hello_world()

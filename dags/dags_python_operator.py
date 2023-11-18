from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",  # dag name
    schedule="30 6 * * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    def select_fruit():
        fruit = ["apple", "banana", "orange", "avocado"]
        rand_int = random.randint(0, 3)

        print(fruit[rand_int])

    py_t1 = PythonOperator(task_id="py_t1", python_callable=select_fruit)

    py_t1

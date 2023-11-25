from airflow import DAG

# from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.decorators import task

import pendulum

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:

    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id="python_t1",
        python_callable=python_function1,
        op_kwargs={
            "start_date": "{{data_interval_start | ds}}",
            "end_date": "{{data_interval_end | ds}}",
        },
    )

    @task(task_id="python_t2")
    def python_function2(**kwargs):
        print(kwargs)
        print(f"df: {kwargs['ds']}")
        print(f"df: {kwargs['ts']}")
        print(f"data_interval_start: {kwargs['data_interval_start']}")
        print(f"data_interval_end: {kwargs['data_interval_end']}")
        print(f"task_instance: {kwargs['ti']}")

    python_t1 >> python_function2()

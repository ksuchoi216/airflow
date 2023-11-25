from airflow import DAG
import datetime
import pendulum

# from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",  # dag name
    schedule="30 6 * * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    @task(task_id="python_xcom_push_t1")
    def xcom_push1(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="res1", value="value 1")
        ti.xcom_push(key="res2", value=[1, 2, 3])

    @task(task_id="python_xcom_push_t2")
    def xcom_push2(**kwargs):
        ti = kwargs["ti"]
        ti.xcom_push(key="res1", value="value 2")
        ti.xcom_push(key="res2", value=[1, 2, 3, 4])

    @task(task_id="python_xcom_pull_t3")
    def xcom_pull(**kwargs):
        ti = kwargs["ti"]
        value1 = ti.xcom_pull(key="res1")
        value2 = ti.xcom_pull(key="res2", task_ids="python_xcom_push_t1")
        print(value1)
        print(value2)

    xcom_push1() >> xcom_push2() >> xcom_pull()

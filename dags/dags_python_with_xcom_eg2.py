from airflow import DAG
import datetime
import pendulum

# from airflow.operators.python import PythonOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",  # dag name
    schedule="30 6 * * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    @task(task_id="python_xcom_push_by_return")
    def xcom_push_result(**kwargs):
        return "Success"

    @task(task_id="python_xcom_pull_t1")
    def xcom_pull_t1(**kwargs):
        ti = kwargs["ti"]
        value1 = ti.xcom_pull(task_ids="python_xcom_push_by_return")
        print("xcom_pull: ", value1)

    @task(task_id="python_xcom_pull_t2")
    def xcom_pull_t2(status, **kwargs):
        print("status: ", status)

    python_xcom_push_by_return = xcom_push_result()
    xcom_pull_t2(python_xcom_push_by_return)
    python_xcom_push_by_return >> xcom_pull_t2("status!!")

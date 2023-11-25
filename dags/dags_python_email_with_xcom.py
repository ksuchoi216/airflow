from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator
from airflow.decorators import task


with DAG(
    dag_id="dags_python_email_with_xcom",  # dag name
    schedule="0 8 1 * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:

    @task(task_id="python_task")
    def some_logic(**kwargs):
        from random import choice

        return choice(["Success", "Fail"])

    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="ksuchoi216@gmail.com",
        cc="kc.kyungsu.choi@gmail.com",
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} some_logic 처리결과',
        html_content='{{data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과 <br> \
                                {{ti.xcom_pull(task_ids="python_task")}} 했습니다 <br>',
    )

    some_logic() >> send_email_task

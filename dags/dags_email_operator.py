from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",  # dag name
    schedule="0 8 1 * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    send_email_task = EmailOperator(
        task_id="send_email_task",
        to="ksuchoi216@gmail.com",
        cc="kc.kyungsu.choi@gmail.com",
        subject="Airflow 성공메일",
        html_content="Airflow 메일입니다",
    )

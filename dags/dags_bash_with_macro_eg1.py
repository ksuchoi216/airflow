import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",  # dag name
    schedule="10 0 L * *",  # m, h, day, month, d of w sat of 1st week
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    # start_date: 전월 말일, end_date: 1일 전
    bash_t1 = BashOperator(
        task_id="bash_t1",
        env={
            "START_DATE": '{{data_interval_start.in_timezone("Asia/Seoul") | ds}}',
            "END_DATE": '{{(data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}',
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )

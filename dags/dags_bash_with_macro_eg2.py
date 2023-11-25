import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg2",  # dag name
    schedule="10 0 * * 6#2",  # Sat of 2nd week
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    # start_date: 2주전 월요일(-14), end_date: 2주전 토요일(-19)
    bash_t2 = BashOperator(
        task_id="bash_t2",
        env={
            "START_DATE": "{{(data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}",
            "END_DATE": "{{(data_interval_end.in_timezone('Asia/Seoul') - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}",
        },
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"',
    )

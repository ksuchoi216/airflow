import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",  # dag name
    schedule="10 0 * * 6#1",  # m, h, day, month, d of w sat of 1st week
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo 'data_interval_end: {{data_interval_end}}'",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        env={
            "START_DATE": "{{data_interval_start | ds}}",
            "END_DATE": "{{data_interval_end | ds}}",
        },
        bash_command="echo $START_DATE && echo $END_DATE",
    )
    bash_t1 >> bash_t2

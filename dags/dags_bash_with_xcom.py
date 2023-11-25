import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",  # dag name
    schedule="10 0 * * *",  # m, h, day, month, d of w sat of 1st week
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    bash_push = BashOperator(
        task_id="bash_push",
        bash_command="echo START && "
        "echo XCOM_PUSHED "
        "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} && "
        "echo COMPLETE",
    )

    bash_pull = BashOperator(
        task_id="bash_pull",
        env={
            "pushed_value": "{{ti.xcom_pull(key='bash_pushed')}}",
            "return_value": "{{ti.xcom_pull(task_ids='bash_push')}}",
        },
        bash_command="echo $pushed_value && echo $return_value ",
        do_xcom_push=False,
    )
    bash_push >> bash_pull

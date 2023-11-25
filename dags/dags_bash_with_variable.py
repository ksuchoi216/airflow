import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",  # dag name
    schedule="10 0 * * *",  # m, h, day, month, d of w sat of 1st week
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    var_value = Variable.get("sample_key")
    bash_var_1 = BashOperator(
        task_id="bash_var_1", bash_command=f"echo variable: {var_value}"
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2", bash_command="echo variable: {{var.value.sample_key}}"
    )

    bash_var_1 >> bash_var_2

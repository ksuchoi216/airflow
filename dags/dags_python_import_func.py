from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator

# from plugins.common.common_func import get_sftp -> error
from common.common_func import get_sftp


with DAG(
    dag_id="dags_bash_select_fruit",  # dag name
    schedule="30 6 * * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    task_get_sftp = PythonOperator(task_id="task_get_sftp", python_callable=get_sftp)
    task_get_sftp

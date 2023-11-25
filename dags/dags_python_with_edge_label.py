from airflow import DAG
import datetime
import pendulum

from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="dags_python_with_edge_label",  # dag name
    schedule="30 6 * * *",  # m, h, day, month, d of w
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
    # tags=["example", "example2"],
    # params={"example_key": "example_value"},
) as dag:
    empty1 = EmptyOperator(task_id="empty1")
    empty2 = EmptyOperator(task_id="empty2")

    empty1 >> Label("between 1 and 2") >> empty2

    empty3 = EmptyOperator(task_id="empty3")
    empty4 = EmptyOperator(task_id="empty4")
    empty5 = EmptyOperator(task_id="empty5")
    empty6 = EmptyOperator(task_id="empty6")

    (
        empty2
        >> Label("start_branch")
        >> [empty3, empty4, empty5]
        >> Label("end_branch")
        >> empty6
    )

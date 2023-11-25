from airflow import DAG
import pendulum
from airflow.decorators import task

# from airflow.operators.bash import BashOperator
# from airflow.exceptions import AirflowException
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:

    @task_group(group_id="first_group")
    def group_1():
        """task_group decoratore in first group"""

        @task(task_id="inner_func1")
        def inner_func1():
            print("1st task in 1st group")

        @task(task_id="inner_func2")
        def inner_func2():
            print("2nd task in 1st group")

        inner_func1() >> inner_func2()

    def inner_func(**kwargs):
        msg = kwargs.get("msg") or ""
        print(msg)

    with TaskGroup(
        group_id="second_group", tooltip="this task group in second group"
    ) as group_2:

        @task(task_id="inner_func1")
        def inner_func1(**kwargs):
            print("1st task in 2nd group")

        inner_func2 = PythonOperator(
            task_id="inner_func2",
            python_callable=inner_func,
        )
        inner_func1() >> inner_func2

    group_1() >> group_2

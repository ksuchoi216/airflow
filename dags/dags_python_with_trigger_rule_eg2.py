from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:

    @task.branch(task_id="python_branch_task")
    def random_branch():
        import random

        item_lst = ["a", "b", "c"]
        selected_item = random.choice(item_lst)

        if selected_item == "a":
            return "task_a"
        elif selected_item == "b":
            return "task_b"
        elif selected_item == "c":
            return "task_c"

    task_a = BashOperator(task_id="task_a", bash_command="echo upstream1")

    @task(task_id="task_b")
    def task_b():
        print("success!!!")

    @task(task_id="task_c")
    def task_c():
        print("success!!!")

    @task(task_id="task_d", trigger_rule="none_skipped")
    def task_d():
        print("success!!!")

    random_branch() >> [task_a, task_b(), task_c()] >> task_d()

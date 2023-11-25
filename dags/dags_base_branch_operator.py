from typing import Iterable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.context import Context

import pendulum
from airflow.operators.branch import BaseBranchOperator

with DAG(
    dag_id="dags_base_branch_operator",
    schedule=None,
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:

    class CustomBaseBranchOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random

            print(context)

            item_lst = ["a", "b", "c"]
            selected_item = random.choice(item_lst)

            if selected_item == "a":
                return "task_a"
            elif selected_item in ["b", "c"]:
                return ["task_b", "task_c"]

    def common_func(**kwargs):
        print(kwargs["selected"])

    task_a = PythonOperator(
        task_id="task_a", python_callable=common_func, op_kwargs={"selected": "a"}
    )

    task_b = PythonOperator(
        task_id="task_b", python_callable=common_func, op_kwargs={"selected": "b"}
    )
    task_c = PythonOperator(
        task_id="task_c", python_callable=common_func, op_kwargs={"selected": "c"}
    )

    custom_branch_operator = CustomBaseBranchOperator(task_id="python_branch_task")
    custom_branch_operator >> [task_a, task_b, task_c]

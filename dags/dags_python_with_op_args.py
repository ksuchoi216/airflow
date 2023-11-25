from airflow import DAG

# from airflow.decorators import task
from airflow.operators.python import PythonOperator

import pendulum
from common.common_func import regist

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:
    regist_t1 = PythonOperator(
        task_id="regist_t1",
        python_callable=regist,
        op_args=["KC", "male", "korea", "seoul"],
    )

    regist_t1

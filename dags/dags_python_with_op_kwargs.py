from airflow import DAG

# from airflow.decorators import task
from airflow.operators.python import PythonOperator

import pendulum
from common.common_func import regist2

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    # tags=["example"],
) as dag:
    regist2_t1 = PythonOperator(
        task_id="regist1_t1",
        python_callable=regist2,
        op_args=["KC", "male", "korea", "seoul"],
        op_kwargs={"email": "ksuchoi222@gmail.com", "phone": "010"},
    )

    regist2_t1

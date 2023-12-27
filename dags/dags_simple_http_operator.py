import datetime
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_simple_http_operator",
    start_date=pendulum.datetime(2023, 12, 21, tz="Asia/Seoul"),
    catchup=False,
    schedule=None,
) as dag:
    tb_cycle_station_info = SimpleHttpOperator(
        task_id="tb_cycle_station_info",
        http_conn_id="openapi.seoul.go.kr",
        endpoint="{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/",
        method="GET",
        headers={
            "Content-Type": "application/json",
            "charset": "utf-8",
            "Accept": "*/*",
        }
        # http://openapi.seoul.go.kr:8088/(인증키)/xml/airPolutionMeasuringItem/1/5/
    )

    @task(task_id="python_2")
    def python_2(**kwargs):
        ti = kwargs["ti"]
        rslt = ti.xcom_pull(task_ids="tb_cycle_station_info")  # 위 task 결과 리턴 값
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cycle_station_info >> python_2()

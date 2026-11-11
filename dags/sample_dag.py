from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


# 파이썬 작업에서 실행할 함수 (print가 아니라 return으로 문자열을 돌려준다)
def say_hello():
    return "Hello Airflow"


def say_goodbye():
    return "Goodbye Airflow"


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",  # 매일 1회
    catchup=False,  # 과거 구간은 자동으로 실행하지 않음
) as dag:
    # 시작, 종료: 아무 동작도 하지 않는 task
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    hello = PythonOperator(task_id="hello_task", python_callable=say_hello)
    goodbye = PythonOperator(task_id="goodbye_task", python_callable=say_goodbye)

    # 시작 -> 파이썬 작업 2개 -> 종료 순서로 연결
    start >> hello >> goodbye >> end

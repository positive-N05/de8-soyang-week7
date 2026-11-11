from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


# 앞 작업: 이 DAG 파일의 줄 수를 세서 return 한다 (반환값은 XCom에 return_value로 저장됨)
def count_lines(ti):
    # 재시도 확인용: 첫 시도(try_number가 1)에서만 일부러 실패시킨다
    if ti.try_number == 1:
        raise Exception("첫 시도라서 일부러 실패시킴")

    with open(__file__, encoding="utf-8") as f:
        lines = f.readlines()
    return len(lines)


# 뒤 작업: 앞 작업의 반환값을 xcom_pull로 받아서 사용한다
def use_value(ti):
    value = ti.xcom_pull(task_ids="count_lines")
    print("received from XCom:", value)
    print("doubled =", value * 2)


with DAG(
    dag_id="xcom_demo_soyang",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 직접 실행할 때만 돈다
    catchup=False,
) as dag:
    count = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,  # 실패하면 최대 2번 더 시도
        retry_delay=timedelta(seconds=10),  # 다시 시도하기 전에 10초 기다림
    )
    use = PythonOperator(task_id="use_value", python_callable=use_value)

    # 앞 작업 -> 뒤 작업 순서로 연결
    count >> use

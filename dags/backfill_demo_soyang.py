import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 결과 파일을 저장할 폴더 (작업이 실행되는 worker 컨테이너 안)
OUTPUT_DIR = "/tmp/backfill_demo"


# 실행마다 자기 logical date(ds, 예: 2026-11-04)를 파일 이름에 넣어서 날짜별로 따로 저장
def write_daily_file(ds):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = f"{OUTPUT_DIR}/daily_{ds}.txt"
    with open(path, "w") as f:
        f.write(f"logical date = {ds}\n")
    print("wrote", path)


# 방금 쓴 파일을 다시 읽어서 내용을 로그에 출력
def verify_file(ds):
    path = f"{OUTPUT_DIR}/daily_{ds}.txt"
    with open(path) as f:
        print(path, "->", f.read().strip())


with DAG(
    dag_id="backfill_demo_soyang",
    start_date=datetime(2026, 11, 4),  # 작업하는 날 기준 7일 전 날짜로 고정
    schedule="@daily",  # 매일 1회
    catchup=True,  # start_date부터 지금까지 지나간 날짜의 실행을 자동으로 채움
) as dag:
    write = PythonOperator(task_id="write_daily_file", python_callable=write_daily_file)
    verify = PythonOperator(task_id="verify_file", python_callable=verify_file)

    # 파일 쓰기 -> 파일 확인 순서로 연결
    write >> verify

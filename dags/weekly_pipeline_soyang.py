import os
from datetime import datetime

import boto3
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# 작업 파일을 둘 컨테이너 안의 폴더
WORK_DIR = "/tmp/weekly_pipeline"
LOCAL_CSV = f"{WORK_DIR}/netflix_titles.csv"
OUTPUT_DIR = f"{WORK_DIR}/silver"


# 1) 다운로드: boto3로 s3://{버킷}/bronze/netflix_titles.csv를 컨테이너 안으로 내려받기
def download_csv():
    # 버킷 이름은 코드에 적지 않고 환경변수(S3_BUCKET)로 받는다
    bucket = os.environ["S3_BUCKET"]
    os.makedirs(WORK_DIR, exist_ok=True)

    # 키는 코드에 없고, 컨테이너에 연결한 AWS CLI 자격 증명을 boto3가 읽어서 사용
    s3 = boto3.client("s3")
    s3.download_file(bucket, "bronze/netflix_titles.csv", LOCAL_CSV)
    print(f"downloaded s3://{bucket}/bronze/netflix_titles.csv -> {LOCAL_CSV}")
    print("file size =", os.path.getsize(LOCAL_CSV), "bytes")


# 3) 업로드: 집계 결과를 s3://{버킷}/silver/{오늘 날짜}/에 올리고 목록과 개수 출력
def upload_silver():
    bucket = os.environ["S3_BUCKET"]
    today = datetime.now().strftime("%Y-%m-%d")  # 예: 2026-01-31
    prefix = f"silver/{today}/"
    s3 = boto3.client("s3")

    # Spark가 만든 폴더에서 parquet 파일만 골라서 업로드
    for name in sorted(os.listdir(OUTPUT_DIR)):
        if name.endswith(".parquet"):
            s3.upload_file(f"{OUTPUT_DIR}/{name}", bucket, prefix + name)

    # 올라간 결과 확인: 목록과 개수 출력
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    for obj in objects:
        print(f"s3://{bucket}/{obj['Key']}  {obj['Size']} bytes")
    print("uploaded files =", len(objects))


with DAG(
    dag_id="weekly_pipeline_soyang",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 직접 실행할 때만 돈다
    catchup=False,
) as dag:
    download = PythonOperator(task_id="download_csv", python_callable=download_csv)

    # 2) 집계: spark-submit으로 jobs/transform.py 실행 (마지막 인자 2015가 기준 연도)
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit --master 'local[*]' "
            f"/opt/airflow/dags/jobs/transform.py {LOCAL_CSV} {OUTPUT_DIR} 2015"
        ),
    )

    upload = PythonOperator(task_id="upload_silver", python_callable=upload_silver)

    # 다운로드 -> 집계 -> 업로드 순서로 연결
    download >> transform >> upload

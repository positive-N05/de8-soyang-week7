import csv
import os
import sys

import boto3

# 버킷 이름은 코드에 적지 않고 실행 인자로 받는다
# 사용법: python s3_download.py <버킷이름>
bucket = sys.argv[1]
prefix = "bronze/"
key = "bronze/netflix_titles.csv"
local_path = "data/netflix_titles.csv"

# AWS CLI에 설정한 IAM 사용자 자격 증명을 그대로 사용
s3 = boto3.client("s3")

# [1] bronze/ 아래 객체 목록과 각 객체의 크기 출력
print(f"[1] list s3://{bucket}/{prefix}")
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
for obj in response.get("Contents", []):
    print(f"    {obj['Key']:<35} {obj['Size']:>12,} bytes")

# [2] netflix_titles.csv를 data/ 폴더 아래로 다운로드
os.makedirs("data", exist_ok=True)
print(f"[2] download s3://{bucket}/{key} -> {local_path}")
s3.download_file(bucket, key, local_path)
print(f"    downloaded ({os.path.getsize(local_path):,} bytes)")

# [3] 내려받은 파일의 행 수 출력
#     csv 모듈로 읽어서 따옴표 안의 줄바꿈은 한 행으로 세고, 헤더는 빼고 센다
with open(local_path, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)  # 헤더 건너뛰기
    rows = 0
    for row in reader:
        rows += 1
print(f"[3] rows {rows:,}")

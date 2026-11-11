# Q5: 공식 Airflow 이미지에 JDK와 파이썬 패키지를 더한 커스텀 이미지
FROM apache/airflow:3.4.0

# 1) JDK 설치 (apt는 root 권한으로 실행)
#    베이스 이미지(Debian 13)에 있는 JDK 중 Spark 4가 지원하는 21 버전을 설치
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2) requirements.txt로 파이썬 패키지 설치 (pip는 airflow 사용자로 실행)
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

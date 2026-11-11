# de8-soyang-week7

메타코드 데이터엔지니어링 8기 양수오의 7주차 과제 레포지토리입니다.

## 실습 환경

Airflow는 직접 빌드한 커스텀 이미지(week7-airflow:soyang)로 실행했습니다.
Docker Compose로 Airflow 3.4.0(CeleryExecutor)과 Spark 3.5.7 Standalone 클러스터를 따로 띄웠습니다.

## 회고

S3에서 받은 데이터를 Spark로 집계해 다시 S3에 올리는 파이프라인을 만들어 봤습니다.
XCom, 재시도, 백필처럼 Airflow를 운영할 때 필요한 기능을 직접 확인했습니다.

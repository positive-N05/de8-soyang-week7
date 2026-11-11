import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, explode, split, trim

# 사용법: spark-submit transform.py <입력 CSV 경로> <출력 폴더> [기준 연도]
input_path = sys.argv[1]
output_path = sys.argv[2]
# 기준 연도는 인자로 받고, 주지 않으면 2015
base_year = int(sys.argv[3]) if len(sys.argv) > 3 else 2015

spark = SparkSession.builder \
    .appName("transform_soyang") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()

# 1) CSV 읽기
#    description 같은 값 안에 줄바꿈과 쉼표가 있어서 multiLine, escape 옵션을 준다
df = spark.read \
    .option("header", True) \
    .option("multiLine", True) \
    .option("escape", '"') \
    .csv(input_path)
print("input rows =", df.count())

# 2) release_year가 기준 연도 이상인 행만 남기기
recent = df.filter(col("release_year").cast("int") >= base_year)
print(f"release_year >= {base_year} rows =", recent.count())

# 3) listed_in(쉼표로 구분된 장르 목록)을 장르 하나당 한 행으로 펼치고 앞뒤 공백 제거
genres = recent.withColumn("genre", explode(split(col("listed_in"), ",")))
genres = genres.withColumn("genre", trim(col("genre")))

# 4) type x 장르별 작품 수 집계
result = genres.groupBy("type", "genre").agg(count("*").alias("title_count"))

# 5) 집계 행 수 출력 (채점 기준이 되는 값)
result_rows = result.count()
print("result rows =", result_rows)
result.orderBy("type", "genre").show(result_rows, truncate=False)

# 6) parquet(snappy 압축)로 컨테이너 안의 로컬 경로에 저장 (파일 하나로 모아서 저장)
result.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(output_path)
print("saved to", output_path)

spark.stop()

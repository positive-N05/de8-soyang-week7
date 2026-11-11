from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, explode, split

spark = SparkSession.builder \
    .appName("WordCount_soyang") \
    .config("spark.log.level", "WARN") \
    .getOrCreate()
# spark.log.level=WARN: INFO 로그를 줄여서 결과 표가 한 화면에 보이게 함

# 1) 텍스트 파일 읽기 (한 줄이 한 행, 컬럼 이름은 value)
lines = spark.read.text("/opt/spark/data/wordcount.txt")

# 2) 공백(" ") 기준으로만 단어 분리 (소문자 변환, 구두점 제거는 하지 않음)
#    빈 줄은 빈 문자열("")이 되므로 걸러낸다
words = lines.select(explode(split(col("value"), " ")).alias("word"))
words = words.filter(col("word") != "")

print("total_words =", words.count())
print("distinct_words =", words.distinct().count())

# 3) 단어별 개수 집계
word_count = words.groupBy("word").count()

# 4) 빈도 내림차순 상위 20개 (빈도가 같으면 단어 순으로 정렬)
word_count.orderBy(desc("count"), "word").show(20)

spark.stop()

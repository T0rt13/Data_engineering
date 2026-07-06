# activate .venv, then run spark-submit ./tweets.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, udtf
from pyspark.sql.types import IntegerType
import re

spark = SparkSession.builder.appName("HashtagExtractor").getOrCreate()

data = [("learning #AI with #ML!",), ("Explore #DataScience",), ("No hashtags here",)]
df = spark.createDataFrame(data, ["text"])

spark.sparkContext.setLogLevel("WARN")

@udf(returnType=IntegerType())
def count_hashtags(text: str):
    if text:
        # little w for words, big W for all other characters (punctuation)
        return len(re.findall(r"#\w+", text))

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)

spark.udf.register("count_hashtags", count_hashtags)
spark.udtf.register("HashtagExtractor", HashtagExtractor)

spark.sql("SELECT count_hashtags('Welcome to #ApachSpark and #BigData') AS hashtag_count").show()

df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()

spark.sql("SELECT * FROM HashtagExtractor('Welcom to #ApacheSpark and #BigData!')").show()

df.createOrReplaceTempView("tweets")

# LATERAL allows function/subquery on the right side see columns from the table on the left side
spark.sql("SELECT text, hashtag FROM tweets, LATERAL HashtagExtractor(text)").show()

spark.stop()
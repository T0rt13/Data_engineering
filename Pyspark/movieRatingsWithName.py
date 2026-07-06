# Uses broadcast

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, StringType
from pyspark.sql.functions import udf

# -> is a type hint, declares intended return type
def loadMovieNames() -> dict[int, str]:
    movieNames = {}

    with open("./ml-100k/u.item", "r", encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split("|")
            movieNames[int(fields[0])] = fields[1]
        return movieNames
    
spark = SparkSession.builder.appName("popularMovies").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([
    StructField("user", IntegerType()),
    StructField("movieID", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("timestamp", LongType())
])

movieRatingsDF = spark.read.option("sep", "\t").schema(schema).csv("./ml-100k/u.data")

movieRatingCounts = movieRatingsDF.groupBy("movieID").count()

@udf
def lookupName(movieID: int) -> str:
    return nameDict.value.get(movieID, "Unknown")

movieCountsWithNames = movieRatingCounts.withColumn("movieTitle", lookupName(F.col("movieID")))
sortedMovieCountsWithNames = movieCountsWithNames.orderBy(F.desc("count"))

sortedMovieCountsWithNames.show(10, False)


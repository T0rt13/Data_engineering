from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("Superheres").getOrCreate()

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType())
])

names = spark.read.schema(schema).option("sep", " ").csv("./MarvelNames.txt")
lines = spark.read.text("./MarvelGraph.txt")

# Parse each line of the input data.
# The first value is the person's ID, and the remaining values are the IDs
# of people they are connected to. Count how many connections each person has.
connections = lines.withColumn("id", F.split(F.col("value"), " ")[0]) \
    .withColumn("connections", F.size(F.split(F.col("value"), " ")) - 1) \
    .groupBy("id").agg(F.sum("connections").alias("connections")) 

# Sort all users by their total number of connections in descending order
# and retrieve the user with the highest connection count.
mostPopular = connections.sort(F.col("connections").desc()).first()
mostPopularName = names.filter(F.col("id") == mostPopular[0]).select("name").first()

print()
print(str(mostPopularName[0]) + " is most popular hero, with " + str(mostPopular[1]) + " appearances!")
print()

spark.stop()

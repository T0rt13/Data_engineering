from pyspark.sql import SparkSession, functions as F
from math import sqrt
import sys

# Note: see output/movie-sims/ for Parquet file downloaded from S3

# ----------------------------
# Compute cosine similarity
# ----------------------------
def computeCosineSimilarity(data):
    # Compute xx, xy and yy columns
    pairScores = data \
      .withColumn("xx", F.col("rating1") * F.col("rating1")) \
      .withColumn("yy", F.col("rating2") * F.col("rating2")) \
      .withColumn("xy", F.col("rating1") * F.col("rating2")) 

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores \
      .groupBy("movie1", "movie2") \
      .agg( \
        F.sum(F.col("xy")).alias("numerator"), \
        (F.sqrt(F.sum(F.col("xx"))) * F.sqrt(F.sum(F.col("yy")))).alias("denominator"), \
        F.count(F.col("xy")).alias("numPairs")
      )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs)
    result = calculateSimilarity \
      .withColumn("score", \
        F.when(F.col("denominator") != 0, F.col("numerator") / F.col("denominator")) \
          .otherwise(0) \
      ).select("movie1", "movie2", "score", "numPairs")

    return result


# ----------------------------
# Load movie names from S3
# ----------------------------
def loadMovieNames(spark, path):
    lines = spark.read.text(path)

    parts = F.split(F.col("value"), "::")

    return lines.select(
        parts[0].cast("int").alias("movieId"),
        parts[1].alias("title")
    )

# ----------------------------
# Load ratings from S3
# ----------------------------
def loadRatings(spark, path):
    lines = spark.read.text(path)

    parts = F.split(F.col("value"), "::")

    return lines.select(
        parts[0].cast("int").alias("userId"),
        parts[1].cast("int").alias("movieId"),
        parts[2].cast("int").alias("rating")
    )


# ----------------------------
# Get movie name by given movie id 
# ----------------------------
def getMovieName(movieNames, movieId):
    result = movieNames.filter(F.col("movieId") == movieId) \
        .select("title").collect()[0]

    return result[0]


# ----------------------------
# Main Spark setup
# ----------------------------
spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# S3 paths (CHANGE THIS)
# ----------------------------
MOVIES_PATH = "s3a://mickey-mouse-882470096331-us-east-2-an/ml-1m/movies.dat"
RATINGS_PATH = "s3a://mickey-mouse-882470096331-us-east-2-an/ml-1m/ratings.dat"
OUTPUT_PATH = "s3a://mickey-mouse-882470096331-us-east-2-an/output/movie-sims"

# ----------------------------
# Load and broadcast movie names
# ----------------------------
print("Loading movie names from S3...")
movies = loadMovieNames(spark, MOVIES_PATH)


# ----------------------------
# Load ratings from S3
# ----------------------------
print("Loading ratings from S3...")
ratings = loadRatings(spark, RATINGS_PATH)


# ----------------------------
# Build movie pairs
# ----------------------------
# repartition along userID
ratingsPartitioned = ratings.repartition(100, "userId")

# self join ratings to find every combination
r1 = ratingsPartitioned.alias("r1")
r2 = ratingsPartitioned.alias("r2")
joinedRatings = r1.join(
    r2, F.col("r1.userId") == F.col("r2.userId")
)

# Filter duplicate movie pairs
uniqueJoinedRatings = joinedRatings.filter(F.col("r1.movieId") < F.col("r2.movieId"))

# Select movie pairs and rating pairs
moviePairs = uniqueJoinedRatings.select(
    F.col("r1.movieId").alias("movie1"),
    F.col("r2.movieId").alias("movie2"),
    F.col("r1.rating").alias("rating1"),
    F.col("r2.rating").alias("rating2")
).repartition(100)


# ----------------------------
# Compute similarities
# ----------------------------
moviePairSimilarities = computeCosineSimilarity(moviePairs).persist()


# Save results to Parquet
moviePairSimilarities.write.mode("overwrite").parquet(OUTPUT_PATH)


# ----------------------------
# Query similar movies
# ----------------------------
if len(sys.argv) > 1:

    # the 0 index always contains the script name, so the second one '1', has our first cli param
    movieID = int(sys.argv[1])

    scoreThreshold = 0.97
    coOccurrenceThreshold = 50

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter( \
        ((F.col("movie1") == movieID) | (F.col("movie2") == movieID)) & \
        (F.col("score") > scoreThreshold) & (F.col("numPairs") > coOccurrenceThreshold))

    results = (
        filteredResults.withColumn(
            "similarMovieId",
            F.when(  # Find the other movie that is similar
                F.col("movie1") == movieID,
                F.col("movie2")
            ).otherwise(F.col("movie1"))
        )
        .join(  # add titles by joining with broadcasted movie DataFrame
            F.broadcast(movies),
            F.col("similarMovieId") == F.col("movieId")
        )
        .select( # keep only this info
            "title",
            "score",
            "numPairs"
        )
        # sort by similarity score, and keep the top 10
        .orderBy(F.col("score").desc())
        .limit(10)
    )

print("Top 10 similar movies")

results.show()
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.sql.functions import col, when, count, avg, max
import sys

# Create a SparkSession
spark = SparkSession.builder \
    .appName("NBA_KMeans") \
    .getOrCreate()

# Read the CSV data from stdin locally
# input_data = sys.stdin.read().split("\n")
# rdd = spark.sparkContext.parallelize(input_data)
# df = spark.read.csv(rdd, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Read the CSV data in the cluster
input_file = sys.argv[1]
df = spark.read.csv(input_file, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Filter out records with null values in the required columns
df = df.filter(col("SHOT_DIST").isNotNull() & col("CLOSE_DEF_DIST").isNotNull() & col("SHOT_CLOCK").isNotNull())

# Create a feature vector assembler
assembler = VectorAssembler(inputCols=["SHOT_DIST", "CLOSE_DEF_DIST", "SHOT_CLOCK"], outputCol="features")

# Transform the data
data = assembler.transform(df)

# Train the KMeans model with 4 clusters
kmeans = KMeans(k=4, seed=1)
model = kmeans.fit(data)

# Print the cluster centroids
print("Cluster Centroids:")
centers = model.clusterCenters()
for i, center in enumerate(centers):
    print(f"Zone {i}: {center}")

# Make predictions
predictions = model.transform(data)

# Calculate hit rate for each player and zone
player_zones = predictions.groupBy("player_name", "prediction") \
    .agg(
        count("*").alias("total_shots"),
        avg(when(col("SHOT_RESULT") == "made", 1).otherwise(0)).alias("hit_rate")
    ) \
    .orderBy("player_name", "prediction")

# Find the maximum hit rate for each player
max_hit_rates = player_zones.groupBy("player_name").agg(max("hit_rate").alias("max_hit_rate"))

# Join the maximum hit rates with player_zones to find the best zone
best_zones = player_zones.join(max_hit_rates, on="player_name") \
    .filter(col("hit_rate") == col("max_hit_rate")) \
    .groupBy("player_name") \
    .agg(
        max("hit_rate").alias("best_hit_rate"),
        avg("prediction").cast("integer").alias("best_zone")
    )


# Filter the results for the specified players
specified_players = ["james harden", "chris paul", "stephen curry", "lebron james"]
result = best_zones.filter(col("player_name").isin(specified_players))

# Show the best zone for each specified player
result.select("player_name", "best_zone", "best_hit_rate").show()

# Stop the SparkSession
spark.stop()

from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col, count
import sys

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ParkingTicketAnalysisKMeans") \
    .getOrCreate()

# Read the CSV data from stdin locally
# input_data = sys.stdin.read().split("\n")
# rdd = spark.sparkContext.parallelize(input_data)
# df = spark.read.csv(rdd, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Read the CSV data in the cluster
input_file = sys.argv[1]
df = spark.read.csv(input_file, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Preprocess the data
data = df.select("Street Code1", "Street Code2", "Street Code3", "Vehicle Color", "Violation Time")
data = data.na.drop()  # Drop rows with missing values

# Create a feature vector
assembler = VectorAssembler(inputCols=["Street Code1", "Street Code2", "Street Code3"], outputCol="features")
data = assembler.transform(data)

# Train the K-Means model
kmeans = KMeans(k=3, seed=1)  # Specify the number of clusters (k) and random seed
model = kmeans.fit(data)

# Add the prediction column to the data DataFrame
data = model.transform(data)

# Question 1: Predict the probability of getting a ticket for a Black vehicle
new_data = spark.createDataFrame([(34510, 10030, 34050, "BLACK")], ["Street Code1", "Street Code2", "Street Code3", "Vehicle Color"])
new_data = assembler.transform(new_data)
new_feature_vector = new_data.select("features").first().features
cluster = model.predict(new_feature_vector)
probability = data.filter(data.prediction == cluster).filter(data["Vehicle Color"] == "BLACK").count() / data.filter(data.prediction == cluster).count()
print(f"Probability of getting a ticket for a Black vehicle at the given location: {probability}")


# Question 2: Recommend a parking location near Lincoln Center
lincoln_center_data = data.filter(col("Street Code1").between(5880, 11110))  # Assuming Lincoln Center is within this range
best_cluster = lincoln_center_data.groupBy("prediction").agg(count("*").alias("count")).orderBy("count").first()[0]
recommended_location = lincoln_center_data.filter(col("prediction") == best_cluster).first()
print(f"Recommended parking location near Lincoln Center: Street Code1={recommended_location['Street Code1']}, Street Code2={recommended_location['Street Code2']}, Street Code3={recommended_location['Street Code3']}")

# Stop the SparkSession
spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when, concat, lit
import sys

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ParkingTicketAnalysis") \
    .getOrCreate()

# Read the CSV data from stdin
# input_data = sys.stdin.read().split("\n")
# rdd = spark.sparkContext.parallelize(input_data)
# df = spark.read.csv(rdd, header=True, inferSchema=True, sep=",", quote='"', escape='"')
input_file = sys.argv[1]
df = spark.read.csv(input_file, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Extract the hour from the Violation Time column
df = df.withColumn("Violation Hour", substring(col("Violation Time"), 1, 2))

# Convert the hour to integer and adjust for AM/PM
df = df.withColumn("Violation Hour", concat(col("Violation Hour"), when(col("Violation Time").endswith("A"), lit("A")).otherwise(lit("P"))))

# Group by the violation hour and count the number of tickets
ticket_counts = df.groupBy("Violation Hour").count().orderBy("Violation Hour")

# Show the ticket counts for each hour
ticket_counts.show(24)

# Find the hour with the maximum number of tickets
max_tickets_row = ticket_counts.orderBy(col("count").desc()).first()
if max_tickets_row is not None:
    max_tickets_hour = max_tickets_row["Violation Hour"]
    print("Hour with the most tickets issued:", max_tickets_hour)
else:
    print("No data found.")

# Stop the SparkSession
spark.stop()
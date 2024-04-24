from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, when, concat, lit
import sys

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ParkingTicketAnalysis") \
    .getOrCreate()

# Read the CSV data from stdin locally
# input_data = sys.stdin.read().split("\n")
# rdd = spark.sparkContext.parallelize(input_data)
# df = spark.read.csv(rdd, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Read the CSV data in the cluster
input_file = sys.argv[1]
df = spark.read.csv(input_file, header=True, inferSchema=True, sep=",", quote='"', escape='"')

# Filter out null values in the "Violation Time" column
df = df.filter(col("Violation Time").isNotNull())

# Extract the hour from the Violation Time column
df = df.withColumn("Violation Hour", substring(col("Violation Time"), 1, 2))

# Convert the hour to integer and adjust for AM/PM
df = df.withColumn("Violation Hour", concat(col("Violation Hour"), when(col("Violation Time").endswith("A"), lit("A")).otherwise(lit("P"))))

# Group by the violation hour and count the number of tickets
ticket_counts = df.groupBy("Violation Hour").count().orderBy("Violation Hour")

# Sort the ticket counts by count in descending order
sorted_ticket_counts = ticket_counts.orderBy(col("count").desc())

# Show the top 5 hours with the most tickets
sorted_ticket_counts.show(5)

# Find the hour with the maximum number of tickets
max_tickets_row = sorted_ticket_counts.first()
if max_tickets_row is not None:
    max_tickets_hour = max_tickets_row["Violation Hour"]
    max_tickets_count = max_tickets_row["count"]
    print("Hour with the most tickets issued: {} ({}A tickets)".format(max_tickets_hour[:-1], max_tickets_count))
else:
    print("No data found.")

# Stop the SparkSession
spark.stop()
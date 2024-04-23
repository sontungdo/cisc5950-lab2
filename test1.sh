#!/bin/bash
source ../env.sh
/usr/local/hadoop/bin/hdfs dfs -rm -r /parking-tickets/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /parking-tickets/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../data/nyc.csv /parking-tickets/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part1.py hdfs://$SPARK_MASTER:9000/parking-tickets/input/

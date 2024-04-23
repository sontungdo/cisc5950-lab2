#!/bin/bash
source ../env.sh

/usr/local/hadoop/bin/hdfs dfs -rm -r /nba-data/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /nba-data/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ../data/nba.csv /nba-data/input/
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./part2.py hdfs://$SPARK_MASTER:9000/nba-data/input/

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("ClickstreamAnalysis").getOrCreate()

# Load data from HDFS
df = spark.read.csv("hdfs://namenode:9000/user/data/clickstream.csv", header=True, inferSchema=True)

# Data Transformation
action_counts = df.groupBy("action").count()

# Show Results
action_counts.show()

# Write Results to Elasticsearch
action_counts.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "elasticsearch") \
    .option("es.port", "9200") \
    .option("es.resource", "clickstream_analysis") \
    .mode("overwrite") \
    .save()


# docker exec -it spark-master bash
# /spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3 clickstream_analysis.py

# Verify Data in Elasticsearch
# curl -X GET "http://localhost:9200/clickstream_analysis/_search?pretty"

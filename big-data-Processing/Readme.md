
# Workshop Instructions:

### Step 1: Prepare Environment
Ensure Docker and Docker Compose are installed:
```
docker --version
docker-compose --version
```

### Step 2: Clone Project
```
git clone https://github.com/silwalprabin/big-data-tools
cd big-data-tools
```

### Step 3: Start Services
```
docker-compose up -d
```

### Step 4: Verify Running Services
- Hadoop Namenode UI: [http://localhost:9870](http://localhost:9870)
- Spark UI: [http://localhost:8080](http://localhost:8080)
- Kibana: [http://localhost:5601](http://localhost:5601)
- Kafka: Connect at `localhost:9092`


### Step 5: Upload Sample Data to HDFS
```
docker cp clickstream.csv namenode:/
docker exec -it namenode /bin/bash
hdfs dfs -mkdir -p /user/data
hdfs dfs -put /path/on/host/clickstream.csv /user/data/clickstream.csv
hdfs dfs -ls /user/data

hdfs dfs -mkdir /input
echo "Hello Big Data" > /tmp/sample.txt
hdfs dfs -put /tmp/sample.txt /input/
hdfs dfs -ls /input

```

### Step 6: Run Spark Job on Sample Data
```
docker cp clickstream_analysis.py spark-master:/
docker exec -it spark-master bash

/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.4.3 clickstream_analysis.py
```

<!-- 
```
docker exec -it spark-master /bin/bash
/spark/bin/spark-submit --class org.apache.spark.examples.JavaWordCount \
--master spark://spark-master:7077 /spark/jars/spark-examples_2.12-3.1.2.jar \
/input/sample.txt
``` -->

# Verify Data in Elasticsearch (verify if the data is indexed)
curl -X GET "http://localhost:9200/clickstream_analysis/_search?pretty"


### Step 7: Test Kafka Producer/Consumer
```
docker exec -it kafka /bin/bash
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-console-producer.sh --broker-list localhost:9092 --topic test
>Hello Kafka
>^C
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

### Step 8: Test Elasticsearch & Kibana
```
curl -X POST "localhost:9200/workshop/_doc/1" -H 'Content-Type: application/json' -d'{"message": "Hello Elasticsearch"}'
curl -X GET "localhost:9200/workshop/_doc/1"
```
- Access Kibana at [http://localhost:5601](http://localhost:5601) to visualize data.

## Setup kibana::
go to elasticsearch docker::
elasticsearch@4a72f019a92f:~$ bin/elasticsearch-create-enrollment-token --scope kibana
eyJ2ZXIiOiI4LjQuMyIsImFkciI6WyIxOTIuMTY4LjEwNy42OjkyMDAiXSwiZmdyIjoiZmExZTkxNjBhMTE5MzExMGM3NDY5YTY3ZGVkMzdiMGU3MzRlNWQ2MDczYmRhMzhhYTI4NmEzNjdjMGEyMTA1YSIsImtleSI6IjV4SWhGcFFCdHl2c2dGNFVzcnd0OndpdFZVN3diUllLMFRtZHg4OF9iRHcifQ==

go to kibana docker::
kibana@a7748eea0ac4:~$ bin/kibana-verification-code
Your verification code is:  411 171 

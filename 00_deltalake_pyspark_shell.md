
## 1. Environment
- Use 00_spark_kafka_postgres_minio_docker_compose docker compose environment

## 2. Start spark with deltalake support 
``` 
 docker exec -it spark bash
 
pyspark --master local[2] --packages io.delta:delta-spark_2.12:3.2.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
```
- Note: SparkSessionExtensions is an Injection API for Spark SQL developers to extend the capabilities of a SparkSession.
## 2. Creating first Delta table
```
>>> data = spark.range(0,50000)

>>> data.write.format("delta").save("file:///opt/examples/datasets/delta_intro")
```

- Check target location (open another terminal and connect to spark container)
```
ls -l /opt/examples/datasets/delta_intro

total 200
drwxr-xr-x. 3 root root     93 Nov  5 04:15 _delta_log
-rw-r--r--. 1 root root 101384 Nov  5 04:15 part-00000-fd3b545a-4ac7-4c43-bb82-b4f7bcb50719-c000.snappy.parquet
-rw-r--r--. 1 root root 100636 Nov  5 04:15 part-00001-34e76ecd-ceb8-473f-949a-1aad666e8db0-c000.snappy.parquet
```

## 3. What is in the _delta_log folder
```
ls -l /opt/examples/datasets/delta_intro/_delta_log

total 4
-rw-r--r--. 1 root root 1292 Nov  5 04:15 00000000000000000000.json
drwxr-xr-x. 2 root root    6 Nov  5 04:15 _commits
```

## 4. Explore the json 
```
cat /opt/examples/datasets/delta_intro/_delta_log/00000000000000000000.json

{"commitInfo":{"timestamp":1730780127680,"operation":"WRITE","operationParameters":{"mode":"ErrorIfExists","partitionBy":"[]"},"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{"numFiles":"2","numOutputRows":"50000","numOutputBytes":"202020"},"engineInfo":"Apache-Spark/3.5.3 Delta-Lake/3.2.0","txnId":"2ecedecb-0cf3-480c-bc25-77c48019970e"}}
{"metaData":{"id":"cc755aa5-1e2d-4ead-946a-723f1f891a05","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1730780121928}}
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"add":{"path":"part-00000-fd3b545a-4ac7-4c43-bb82-b4f7bcb50719-c000.snappy.parquet","partitionValues":{},"size":101384,"modificationTime":1730780127195,"dataChange":true,"stats":"{\"numRecords\":25000,\"minValues\":{\"id\":0},\"maxValues\":{\"id\":24999},\"nullCount\":{\"id\":0}}"}}
{"add":{"path":"part-00001-34e76ecd-ceb8-473f-949a-1aad666e8db0-c000.snappy.parquet","partitionValues":{},"size":100636,"modificationTime":1730780127195,"dataChange":true,"stats":"{\"numRecords\":25000,\"minValues\":{\"id\":25000},\"maxValues\":{\"id\":49999},\"nullCount\":{\"id\":0}}"}}
```

## 5. Append more data 
```
data2 = spark.range(50000,100000)
data2.write.format("delta").mode("append").save("file:///opt/examples/datasets/delta_intro")
```

## 6. What happened parquets 
```
ls -l /opt/examples/datasets/delta_intro

total 400
drwxr-xr-x. 3 root root    164 Nov  5 04:17 _delta_log
-rw-r--r--. 1 root root 100650 Nov  5 04:17 part-00000-df0e0657-1e3b-4aa1-b357-f80084aeac18-c000.snappy.parquet
-rw-r--r--. 1 root root 101384 Nov  5 04:15 part-00000-fd3b545a-4ac7-4c43-bb82-b4f7bcb50719-c000.snappy.parquet
-rw-r--r--. 1 root root 100637 Nov  5 04:17 part-00001-14402356-95f6-4d8e-922b-42902c790bf3-c000.snappy.parquet
-rw-r--r--. 1 root root 100636 Nov  5 04:15 part-00001-34e76ecd-ceb8-473f-949a-1aad666e8db0-c000.snappy.parquet
```

## 7. New json created 
```
ls -l /opt/examples/datasets/delta_intro/_delta_log/
total 8
-rw-r--r--. 1 root root 1292 Nov  5 04:15 00000000000000000000.json
-rw-r--r--. 1 root root  953 Nov  5 04:17 00000000000000000001.json
drwxr-xr-x. 2 root root    6 Nov  5 04:15 _commits
```


## 8. Read from delta as spark df
```
spark.read.format("delta").load("file:///opt/examples/datasets/delta_intro").show()
+-----+
|   id|
+-----+
|50000|
|50001|
|50002|
|50003|
|50004|
|50005|
|50006|
|50007|
|50008|
|50009|
|50010|
|50011|
|50012|
|50013|
|50014|
|50015|
|50016|
|50017|
|50018|
|50019|
+-----+

```
- Count
```commandline
spark.read.format("delta").load("file:///opt/examples/datasets/delta_intro").count()

100000
```

# Key difference between parquet and delta 
- The key difference is the `_delta_log` folder which is the Delta transaction log. This
transaction log is key to understanding Delta Lake because it is the underlying infrastructure
for many of its most important features including but not limited to ACID
transactions, scalable metadata handling, and time travel.

# What Is the Delta Lake Transaction Log?
- The Delta Lake transaction log (also known as the Delta Log) is an ordered record of
every change that has ever been performed on a Delta Lake table since its inception.

- To show users correct
views of the data at all times, the Delta Lake transaction log serves as a single
source of truth – the central repository that tracks all changes that users make to the
table.

## CRC file. 
For each transaction, there is both a JSON file as well as a CRC file. This file
contains key statistics for the table version (i.e. transaction) allowing Delta Lake to
help Spark optimize its queries.

























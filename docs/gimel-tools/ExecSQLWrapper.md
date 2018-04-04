

## What is ExecSQLWrapper

* The Tool Allows Developer to Write a SQL in a .sql file.
* Run the SQL via copyDataSet tool, which inturn calls a spark application
* Copy Data Set runs in several mode

| Mode | Logic | Notes |
|------|-------|-------|
| batch | runs batch pull from kafka once, writes data to HDFS | <br> Schedule jobs in your scheduler (say UC4) for "not less than" every 30 mins (minimum latency - to avoid any small files problem) <br> This is best fit for HDFS, where you have a downstream dependent job <br> |
| stream | runs a never ending job (streaming) - NOT recommmended for HDFS Sink - creates Small Files |
| batch_iterative | runs a never ending batch daemon | <br> use "batchRecursionMinutes=30" (minimum latency - to avoid any small files problem) <br> This is best fit for HDFS where you have a "ASYNC" downstream consumer, that can detect delta partitions |

--------------------------------------------------------------------------------------------------------------------


## Here is a sample execution (2.2.0)

```bash

#cat copy_dataset_time_partitioning.sql
#insert into GIMEL.SQL.PARAM.TARGET.DB.GIMEL.SQL.PARAM.TARGET
# select kafka_ds.*
#,substr(fl_date,1,4) as yyyy
#,substr(fl_date,6,2) as mm
#,substr(fl_date,9,2) as dd
#from GIMEL.SQL.PARAM.SOURCE.DB.GIMEL.SQL.PARAM.SOURCE kafka_ds

# Copy SQL file to HDFS
hadoop fs -copyFromLocal -f copy_dataset_time_partitioning.sql hdfs:///tmp/copy_dataset_time_partitioning.sql

# Env Variables
export hdfsjar=hdfs:///tmp/gimel_new.tools.jar

# Set source target sql_file
export SOURCE=flights_kafka
export TARGET=flights_elastic
export QUERY_SOURCE_FILE=hdfs:///tmp/copy_dataset_time_partitioning.sql

# Set all other Args
args="gimel.sql.param.source=${SOURCE} \
      gimel.sql.param.target=${TARGET} \
      gimel.sql.param.source.db=PCATALOG \
      gimel.sql.param.target.db=DEFAULT \
      querySourceFile=${QUERY_SOURCE_FILE} \
      mode=batch \
      isBatchRecursionInfinite=true \
      batchRecursionCount=2 \
      batchRecursionMinutes=30 \
      partition_column=timestamp \
      throttle.batch.fetchRowsOnFirstRun=100 \
      gimel.logging.level=CONSOLE \
      gimel.kafka.reader.checkpoint.clear=false \
      gimel.kafka.reader.checkpoint.save=true \
      gimel.query.results.show.rows.only=true \
      gimel.kafka.throttle.batch.maxRecordsPerPartition=100 \
      gimel.kafka.throttle.batch.parallelsPerPartition=2 \
      gimel.kafka.throttle.batch.minRowsPerParallel=1000000 \
      hive.exec.dynamic.partition=true \
      hive.exec.dynamic.partition.mode=nonstrict \
      hive.exec.max.dynamic.partitions.pernode=400
      "


# Start the batch
spark-submit  \
  --master yarn \
   --deploy-mode client \
#  --deploy-mode cluster \
  --conf spark.yarn.principal=$USER \
  --conf spark.yarn.keytab=hdfs:///user/$USER/.keytab/$USER.keytab \
  --conf spark.driver.memory=2g \
  --conf spark.yarn.driver.memoryOverhead=512 \
  --conf spark.executor.memory=2g \
  --conf spark.yarn.executor.memoryOverhead=512 \
  --conf spark.executor.cores=1 \
  --class com.paypal.gimel.tools.ExecSQLWrapper \
  ${hdfsjar} \
  $args
```

_____________________________________________________________________________________________________________


## Here is a sample execution (2.x)

### Create a Kafka Table

```sql
CREATE EXTERNAL TABLE `pcatalog`.`flights_kafka`(`payload` string)
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs:///tmp/flights'
TBLPROPERTIES (
'key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
'numFiles' = '0',
'auto.offset.reset' = 'earliest',
'gimel.kafka.checkpoint.zookeeper.host' = 'zk_host:2181',
'gimel.storage.type' = 'kafka',
'gimel.kafka.throttle.batch.fetchRowsOnFirstRun' = '10000000',
'gimel.kafka.throttle.batch.maxRecordsPerPartition' = '10000000',
'bootstrap.servers' = 'kafka_broker:9092',
'value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
'value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
'gimel.kafka.checkpoint.zookeeper.path' = '/pcatalog/kafka_consumer/checkpoint/flights',
'gimel.kafka.zookeeper.connection.timeout.ms' = '10000',
'key.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
'gimel.kafka.message.value.type' = 'json'
)
  ```

  ### Create a Elastic Search Target Table
  ```sql
  CREATE EXTERNAL TABLE `pcatalog`.`flights_elastic`(`payload` string)
  LOCATION 'hdfs:///tmp/elastic_smoke_test'
  TBLPROPERTIES (
  'numFiles' = '0',
  'gimel.storage.type' = 'ELASTIC_SEARCH',
  'es.index.auto.create' = 'true',
  'es.mapping.date.rich' = 'true',
  'es.port' = '9200',
  'es.resource' = 'flights/data',
  'es.nodes' = 'http://es_host'
  )
  ```

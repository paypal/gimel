* [Kafka API](#kafka-api)
  * [Note](#note)
  * [Limitations](#limitations)
  * [Batch & Streaming Features](#batch-&-streaming-features)
  * [Catalog Properties](#catalog-properties)
  * [Kafka API - Read & Write in Avro Format](#generic-kafka-integration--data-published-as-avro-serialized-messages)
    * [Create Hive Table](#create-hive-table-pointing-to-kafka-topic)
    * [Write to Kafka](#write-to-your-topic-via-kafkadataset)
    * [Read via Kafka Data Set](#read-your-kafka-topic-via-kafkadataset)
    * [Stream via Kafka Data Stream](#read-your-kafka-topic--via-kafkadatastream)
    * [Read via SQL - API](#read-avro-serde-kafka-topic---batch-via-sql)
    * [Stream via SQL - API](#read-avro-serde-kafka-topic---stream-via-sql)
  * [Kafka API - Read & Write in Format - String | JSON | Binary](#simple-kafka-topic-with-string--json--binary-messages)
    * [Create Hive Table](#create-hive-table-pointing-to-kafka-topic-1)
    * [Write to Kafka](#write-to-your-topic-via-kafkadataset-1)
    * [Read via Kafka Data Set](#read-your-kafka-topic-via-kafkadataset-1)
    * [Stream via Kafka Data Stream](#read-your-topic-via-kafkadatastream)
    * [Read via SQL - SQL API](#read-avro-serde-kafka-topic---batch-via-sql)
    * [Stream via SQL - SQL API](#read-avro-serde-kafka-topic---stream-via-sql)


--------------------------------------------------------------------------------------------------------------------


# KAFKA API

## Note

|    Format     | Support                | Differentiating Property |
| ------------- | -----------------------|---------------------------|
| AVRO | Supports READ + WRITE  | 'gimel.kafka.avro.schema.string'='<avroSchemaInJSONFormat>' |
| STRING | Supports READ + WRITE | 'gimel.kafka.message.value.type'='string' |
| JSON | Supports READ + WRITE | 'gimel.kafka.message.value.type'='json' |
| BINARY | Supports READ + WRITE | 'gimel.kafka.message.value.type'='binary' |

--------------------------------------------------------------------------------------------------------------------


## Limitations

* Current implementation does not support batch read / stream from multiple topics.
* Until [SPARK-23636](https://issues.apache.org/jira/browse/SPARK-23636) is resolved - we may have to use num-cores=1 while doing batch fetch from kafka .
  Proposed Solution --> https://github.com/apache/spark/pull/20767

--------------------------------------------------------------------------------------------------------------------


## Batch & Streaming Features

### Checkpointing
* Save The CheckPoint States in Supplied or a Default ZooKeeper.
* Clear the CheckPoint and start over from the beginning via function calls.

### Throttling
* Advanced parameters to consume data from Kafka via Parallelism (Batch only)
* Horizontal Scaling for tasks/executors
* Control Number of messages per executor (Batch only)
* Ability to Control the Number of rows/messages to fetch from kafka in each run. (Batch only)

### Batch & Streaming SQL on Kafka with checkpointing & throttling capabilities
* Express your entire logic in SQL that can combine various datasets from storages such as Hive, HDFS, ES, Kafka
* Explore Data by "select statements"
* Insert Data into Targets by "insert statements" --> On Successful Insert : Consumer's Kafka States are Saved Implicitly in CheckPoint Nodes in Zookeeper

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties

| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| bootstrap.servers | Y | the broker list for kafka | host1:9092,host2:9092 | |
| gimel.kafka.kafka.whitelist.topics | Y | the topic names in kafka separated by comma | flights,flights1 | |
| key.serializer | Y | the kafka key serializer | | org.apache.kafka.common.serialization.StringSerializer |
| value.serializer | Y | the kafka message serializer | org.apache.kafka.common.serialization.StringSerializer | org.apache.kafka.common.serialization.ByteArraySerializer |
| key.deserializer | Y | the kafka key DeSerializer | | org.apache.kafka.common.serialization.StringDeserializer |
| value.deserializer | Y | the kafka message DeSerializer | org.apache.kafka.common.serialization.StringDeserializer | org.apache.kafka.common.serialization.ByteArrayDeserializer |
| zookeeper.connection.timeout.ms | Y | ZooKeeper Time out Millisec | 10000 | |
| group.id  | N | <br>If specified, all readers of this topic will go under same consumer group<br>Thus, only as many consumers as # of partitions can read from this dataset concurrently<br> | assigned random number at runtime |
| gimel.kafka.avro.schema.source | Y | <br>INLINE - indicates avro schema is supplied via gimel.kafka.avro.schema.string<br>CDH - picks up schema from confluent schema registry based on gimel.kafka.avro.schema.source.url<br> | |
| gimel.kafka.avro.schema.string | N | This is a Must if `gimel.kafka.avro.schema.source=INLINE` | Refer Examples below for Avro Data | |
| gimel.kafka.avro.schema.source.url | N | This is a Must if `gimel.kafka.avro.schema.source=CDH` | Refer Examples below for CDH Data | |
| gimel.kafka.avro.schema.source.key | N | This is a Must if `gimel.kafka.avro.schema.source=CDH`, the key to lookup confluent schema registry | Refer Examples below for CDH Data | |
| gimel.kafka.avro.schema.source.wrapper.key | N | <br>This is a Must if `gimel.kafka.avro.schema.source=CDH`<br>CDH has 2 levels of avro serialization, this is the key for confluent schema registry lookup - for the outer schema<br> | Refer Examples below for CDH Data | flights |
| gimel.kafka.checkpoint.zookeeper.host | Y | Zookeeper host where the kafka offsets will be checkpointed for each application | localhost:2181 | |
| gimel.kafka.checkpoint.zookeeper.path | Y | The root note in ZK for checkpointing, additional child paths will be appended based on spark.app.name, spark.user, datasetName to get uniqueness of Zk Node | /gimel/kafka/checkpoints | |
| gimel.kafka.throttle.batch.fetchRowsOnFirstRun | N | The number of last N messages to fetch, if consumer is pulling data first time ever. | 25000000 | 25000000 |
| gimel.kafka.throttle.batch.maxRecordsPerPartition | N | Total Number of Records that will be limited per partition in the Kafka Topic | 10000000 | 10000000 |
| gimel.kafka.throttle.batch.parallelsPerPartition | N | <br>This is a very advanced option to Parallelize the number of connections per Partition.<br>It is best left defaulted. The configuration was introduced during troubleshooting & Performance optimization.<br> | 250 | 250 |
| gimel.kafka.throttle.batch.minRowsPerParallel | N | <br>This is to ensure we do not over subscribe to parallelism & cause very few records to be processed per executor.<br>For instance : providing 100000 messages will be read at minimum from each executor.<br> | 100000 | 100000 |
| gimel.kafka.throttle.batch.targetCoalesceFactor | N | <br>If we are writing to a HDFS sink where small files is an issue, we could set this parameter to 1 so that DF will be repartitioned to 1 executor and written to fewer number of files on HDFS | 1 | Default parallelism in application |
| gimel.kafka.throttle.streaming.isParallel | N | <br>Use in Streaming Mode to parallelize the steps where deserialization happens<br>this feature is recommended if preserving ordering is not a necessity in the sink (like HDFS)<br>Once messages are fetched from kafka, with this flag turned ON, messages can be repartition across executors to process data in parallel via below listed properties<br> | false | true |
| gimel.kafka.throttle.streaming.maxRatePerPartition | N | Max Records to Process per partition | 1000 | 3600 |
| gimel.kafka.throttle.stream.parallelism.factor | N | The number of executors / repartitions to create while deserializing. | 10 | 10 |
| gimel.kafka.custom.offset.range | N | The Custom Offset range and partition to read from | [{\"topic\": \"gimel.demo.flights.json\",\"offsetRange\": [{\"partition\": 0,\"from\": 1200000,\"to\": 1200001}]}] | |

--------------------------------------------------------------------------------------------------------------------



## Generic Kafka Integration | Data Published as Avro Serialized Messages

### Create Hive Table Pointing to Kafka Topic

#### Avro Schema can be INLINE

```sql
CREATE EXTERNAL TABLE `default.user`(
 `name` string,
 `age` int,
 `rev` bigint
)
LOCATION 'hdfs:///tmp/user'
TBLPROPERTIES (
  'gimel.storage.type' = 'KAFKA',
  'bootstrap.servers'='localhost:9092',
  'gimel.kafka.whitelist.topics'='user',
  'gimel.kafka.checkpoint.zookeeper.host'='zk_host:2181',
  'gimel.kafka.checkpoint.zookeeper.path'='/pcatalog/kafka_consumer/checkpoint',
  'key.serializer'='org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer'='org.apache.kafka.common.serialization.ByteArraySerializer',
  'zookeeper.connection.timeout.ms'='10000',
  'auto.offset.reset'='earliest',
  'gimel.kafka.avro.schema.string'=' {
   "type" : "record",
   "namespace" : "default",
   "name" : "user",
   "fields" : [
      { "name" : "name" , "type" : "string" },
      { "name" : "age" , "type" : "int" },
      { "name" : "rev" , "type" : "long" }
   ]}'
   )

```

#### Avro Schema can be fetched from confluent Schema Registry


```sql
CREATE EXTERNAL TABLE `default.user`(
 `name` string,
 `age` int,
 `rev` bigint
)
LOCATION 'hdfs:///tmp/user'
TBLPROPERTIES (
  'gimel.storage.type' = 'KAFKA',
  'bootstrap.servers'='localhost:9092',
  'gimel.kafka.whitelist.topics'='kafka_topic',
  'gimel.kafka.checkpoint.zookeeper.host'='zkhost:2181',
  'gimel.kafka.checkpoint.zookeeper.path'='/pcatalog/kafka_consumer/checkpoint',
  'gimel.kafka.avro.schema.source'='CSR',
  'gimel.kafka.avro.schema.source.url'='http://schemaregistry:8081',
  'gimel.kafka.avro.schema.source.wrapper.key'='flights', -- This is the Schema Lookup Key for Scheme Registry
  'key.serializer'='org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer'='org.apache.kafka.common.serialization.ByteArraySerializer',
  'zookeeper.connection.timeout.ms'='10000',
  'auto.offset.reset'='earliest',
   )

```


### Write to your Topic via KafkaDataSet

```scala
import org.apache.avro.generic.GenericRecord;
import com.paypal.gimel.logger.Logger;
import com.paypal.gimel.datastreamfactory.{StreamingResult, WrappedData};
import com.paypal.gimel._;
import org.apache.spark.rdd._
import org.apache.spark.streaming._
import org.apache.spark._
import org.apache.spark.sql._


// Prepare Test Data
def stringed(n: Int) = s"""{"id": ${n}, "name": "MAC-${n}", "rev": ${n * 10000}}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val df: DataFrame = sparkSession.read.json(rdd)
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//DataSet Name
val datasetName = "default.user"
//write some data
dataset.write(datasetName,df)
```


### Read your Kafka Topic via KafkaDataSet

```scala
//Initiate DataSet
val dataSet: DataSet = DataSet(sparkSession)
//options "can" be used to pick smaller subset of rows
val options = "gimel.kafka.throttle.batch.fetchRowsOnFirstRun=2500:gimel.kafka.throttle.batch.parallelsPerPartition=250:gimel.kafka.throttle.batch.maxRecordsPerPartition=25000000"
//read API
val recsDF = dataSet.read("default.user",options)
// Get Kafka Operator for CheckPoint Operations
val kafkaOperator = dataSet.latestKafkaDataSetReader.get
// If required, clear checkpoint to begin reading from kafka from beginning
kafkaOperator.clearCheckPoint()
// Do some usecase
recsDF.show()
// Save CheckPoint at the end of each batch
kafkaOperator.saveCheckPoint()
```



### Read your Kafka Topic  via KafkaDataStream

```scala
//Specify Batch Interval for Streaming
val batchInterval = 15
//Context
val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))
//Initiate DStream
val dataStream = DataStream(ssc)
// Assign your Dataset Name
val datasetName = "default.user"
//Get Reference to Stream
val streamingResult: StreamingResult = dataStream.read(datasetName)
//Clear CheckPoint if necessary
streamingResult.clearCheckPoint("clear")
//Helper for Clients
  streamingResult.dStream.foreachRDD { rdd =>
    val k: RDD[WrappedData] = rdd
    val count = rdd.count()
    if (count > 0) {
      /**
        * Mandatory | Get Offset for Current Window, so we can checkpoint at the end of this window's operation
        */
      streamingResult.getCurrentCheckPoint(rdd)
      /**
        * Begin | User's Usecases
        */
      //Sample UseCase | Display Count
      println("count is -->")
      println(count)
      //Sample UseCase | Get Avro Generic Record
      val rddAvro: RDD[GenericRecord] = streamingResult.convertBytesToAvro(rdd)
      rddAvro.map(x => x.toString)
      println("sample records from Avro-->")
      rddAvro.map(x => x.toString).take(10).foreach(x => println(x))
      //Sample UseCase | Convert to DataFrame
      val df: DataFrame = streamingResult.convertAvroToDF(sqlContext, rddAvro)
      println("sample records -->")
      df.show(5)
      /**
        * End | User's Usecases
        */
      /**
        * Mandatory | Save Current Window - CheckPoint
        */
      streamingResult.saveCurrentCheckPoint()
    }
  }

//Streaming will start after these steps
dataStream.streamingContext.start()
dataStream.streamingContext.awaitTermination()
```



### Read Avro SerDe Kafka Topic - Batch via SQL

```sql
%%pcatalog-batch select * from default.flights;
```

### Read Avro SerDe Kafka Topic - Stream via SQL
```sql
%%pcatalog-stream select * from default.flights;
```


--------------------------------------------------------------------------------------------------------------------


## Simple Kafka Topic With String | JSON | Binary Messages

### Create Hive Table Pointing to Kafka Topic

```sql
CREATE EXTERNAL TABLE `default.hiveTable`(
    `payload` string
   )
   TBLPROPERTIES (
     'gimel.storage.type' = 'KAFKA',
     'bootstrap.servers'='zk_host:9092',
     'gimel.kafka.whitelist.topics'='your_topic',
     'key.serializer'='org.apache.kafka.common.serialization.StringSerializer',
     'value.serializer'='org.apache.kafka.common.serialization.StringSerializer',
     'zookeeper.connection.timeout.ms'='10000',
     'auto.offset.reset'='earliest',
     'gimel.kafka.checkpoint.zookeeper.host'='zk_host:2181',
     'gimel.kafka.checkpoint.zookeeper.path'='/pcatalog/kafka_consumer/checkpoint',
     'gimel.kafka.message.column.alias'='value',
     'gimel.kafka.message.value.type'='string' -- can be json or binary depending on the messaging format requirement
      )
```


### Write to your Topic via KafkaDataSet

```scala
// Prepare Test Data
def stringed(n: Int) = s"""{"id": ${n}, "name": "MAC-${n}", "rev": ${n * 10000}}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//write some data
dataset.write("default.hiveTable",rdd)
```


### Read your Kafka Topic via KafkaDataSet

```scala
//Initiate DataSet
val dataSet: DataSet = DataSet(sparkSession)
//options "can" be used to pick smaller subset of rows
val options = "gimel.kafka.throttle.batch.fetchRowsOnFirstRun=2500:gimel.kafka.throttle.batch.batch.parallelsPerPartition=250:gimel.kafka.throttle.batch.maxRecordsPerPartition=25000000"
//read API
val recsDF = dataSet.read("default.hiveTable",options)
// Get Kafka Operator for CheckPoint Operations
val kafkaOperator = dataSet.latestKafkaDataSetReader.get
// If required, clear checkpoint to begin reading from kafka from beginning
kafkaOperator.clearCheckPoint()
// Do some usecase
recsDF.show()
// Save CheckPoint at the end of each batch
kafkaOperator.saveCheckPoint()
```



### Read your Topic via KafkaDataStream

```scala
//Specify Batch Interval for Streaming
val batchInterval = 15
//Context
val ssc = new StreamingContext(sc, Seconds(batchInterval.toInt))
//Initiate DStream
val dataStream = DataStream(ssc)
// Assign your Dataset Name
val datasetName = "default.hiveTable"
//Get Reference to Stream
val streamingResult: StreamingResult = dataStream.read(datasetName)
//Clear CheckPoint if necessary
streamingResult.clearCheckPoint("clear")
//Helper for Clients
  streamingResult.dStream.foreachRDD { rdd =>
    val k: RDD[WrappedData] = rdd
    val count = rdd.count()
    if (count > 0) {
      /**
        * Mandatory | Get Offset for Current Window, so we can checkpoint at the end of this window's operation
        */
      streamingResult.getCurrentCheckPoint(rdd)
      /**
        * Begin | User's Usecases
        */
      //Sample UseCase | Display Count
      println("count is -->")
      println(count)
      //Sample UseCase | If Kafka topic is a Simple String Message, use below function to get DF
      streamingResult.convertStringMessageToDF(sqlContext,rdd).show
      /**
        * End | User's Usecases
        */
      /**
        * Mandatory | Save Current Window - CheckPoint
        */
      streamingResult.saveCurrentCheckPoint()
    }
  }

//Streaming will start after these steps
dataStream.streamingContext.start()
dataStream.streamingContext.awaitTermination()
```



### Read Avro SerDe Kafka Topic - Batch via SQL

```sql
%%pcatalog-batch select value from default.hiveTable;
```

### Read Avro SerDe Kafka Topic - Stream via SQL

```sql
%%pcatalog-stream select value from default.hiveTable;
```



--------------------------------------------------------------------------------------------------------------------


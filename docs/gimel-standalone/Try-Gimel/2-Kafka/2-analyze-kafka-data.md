
* [G-SQL](#g-sql)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Kafka](#read-data-from-kafka)
* [Scala API for Catalog Provider-USER](#scala-api-for-catalog-provider-user)
    * [Set Options](#set-options)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Read Data from Kafka](#read-data-from-kafka-1)
* [Scala API for Catalog Provider-HIVE](#scala-api-for-catalog-provider-hive)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Read Data from Kafka](#read-data-from-kafka-2)
   
# G-SQL

## Load Flights Data into Kafka Dataset
```
gsql("insert into pcatalog.flights_kafka_json select * from pcatalog.flights_hdfs")
```

## Cache Flights 
```
gsql("cache table flights select * from  pcatalog.flights_kafka_json")
```

## Read Data from Kafka
```
gsql("select * from flights").show(10)
```
______________________________________________________

# Scala API for Catalog Provider-USER

*Please execute the steps in this section if you have choosen CatalogProvider as USER or if you executed the following command*

```gsql("set gimel.catalog.provider=USER")```
## Set options
```
val datasetKafkaPropsJson = """{ 
                                  "datasetType": "KAFKA",
                                  "fields": [],
                                  "partitionFields": [],
                                  "props": {
                                      "gimel.storage.type":"kafka",
                                		"gimel.kafka.message.value.type":"json",
                                		"gimel.kafka.whitelist.topics":"gimel.demo.flights.json",
                                		"bootstrap.servers":"kafka:9092",
                                		"gimel.kafka.checkpoint.zookeeper.host":"zookeeper:2181",
                                		"gimel.kafka.checkpoint.zookeeper.path":"/pcatalog/kafka_consumer/checkpoint/flights",
                                		"gimel.kafka.zookeeper.connection.timeout.ms":"10000",
                                		"gimel.kafka.throttle.batch.maxRecordsPerPartition":"10000000",
                                		"gimel.kafka.throttle.batch.fetchRowsOnFirstRun":"10000000",    
                                		"auto.offset.reset":"earliest",
                                		"key.serializer":"org.apache.kafka.common.serialization.StringSerializer",
                                		"value.serializer":"org.apache.kafka.common.serialization.StringSerializer",
                                		"key.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
                                		"value.deserializer":"org.apache.kafka.common.serialization.StringDeserializer",
                                		"datasetName":"pcatalog.flights_kafka_json"
                                  }
                              }"""
val datasetHivePropsJson = """{ 
                                      "datasetType": "HDFS",
                                      "fields": [],
                                      "partitionFields": [],
                                      "props": {
                                           "gimel.hdfs.data.format":"csv",
                                           "location":"hdfs://namenode:8020/flights/data",
                                           "datasetName":"pcatalog.flights_hdfs"
                                      }
                                  }"""
val hiveOptions = Map("pcatalog.flights_hdfs.dataSetProperties"->datasetHivePropsJson)
val kafkaOptions = Map("pcatalog.flights_kafka_json.dataSetProperties"->datasetKafkaPropsJson)
```

## Load Flights Data into Kafka Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hivedf = dataSet.read("pcatalog.flights_hdfs",hiveOptions)
val df = dataSet.write("pcatalog.flights_kafka_json",hivedf,kafkaOptions)
df.count
```

## Read Data from Kafka
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_kafka_json",kafkaOptions)
df.show(10)
```
_________________________________________________
# Scala API for Catalog Provider-HIVE

*Please execute the steps in this section if you have choosen CatalogProvider as HIVE or if you executed the following command*

```gsql("set gimel.catalog.provider=HIVE")```

## Load Flights Data into Kafka Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hivedf = dataSet.read("pcatalog.flights_hdfs")
val df = dataSet.write("pcatalog.flights_kafka_json",hivedf)
df.count
```

## Read Data from Kafka
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_kafka_json")
df.show(10)
```
_________________________________________________


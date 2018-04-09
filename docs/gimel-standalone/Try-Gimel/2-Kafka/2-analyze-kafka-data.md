
* [G-SQL](#g--sql)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Kafka](#read-data-from-kafka)
* [Scala API](#scala-api)
    * [Set Options](#set-options)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Read Data from Kafka](#read-data-from-kafka)
   
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

# Scala API

## Set options
```
scala> val datasetPropsJson = { 
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
                              }
scala> val options = Map("pcatalog.flights_hdfs.dataSetProperties"->datasetPropsJson)
```

## Load Flights Data into Kafka Dataset
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val df = dataSet.read("pcatalog.flights_hdfs",options)
scala> df.count
```

## Read Data from Kafka
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val df = dataSet.read("pcatalog.flights_kafka_json",options)
scala> df.show(10)
```
_________________________________________________




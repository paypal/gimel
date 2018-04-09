
* [Bootstrap Data](#bootstrap-data)
      * [Create Kafka Dataset](#create-kafka-dataset)

# Bootstrap Data

### Create Kafka Dataset

```
gsql("""set pcatalog.flights_kafka_json.dataSetProperties={ 
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
}""")

```
_____________________________________________________
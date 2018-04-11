
* [Bootstrap Data](#bootstrap-data)
    * [Create Kafka Dataset](#create-kafka-dataset)

# Bootstrap Data

### Create Kafka Dataset
<table>
  <tbody>
    <tr>
      <th align="center">Catalog Provider</th>
      <th align="center">Command</th>
    </tr>
    <tr>
      <td align="center">USER</td>
      <td align="left">
      
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
     
   </td>
   </tr>
   <tr>
         <td align="center">HIVE</td>
         <td align="left">
      
      drop table if exists pcatalog.flights_kafka_json;   
      CREATE EXTERNAL TABLE `pcatalog.flights_kafka_json`(
        `payload` string)
      ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
      STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      LOCATION
        'hdfs://namenode:8020/tmp/flights'
      TBLPROPERTIES (
        'auto.offset.reset'='earliest',
        'bootstrap.servers'='kafka:9092',
        'gimel.kafka.checkpoint.zookeeper.host'='zookeeper:2181',
        'gimel.kafka.checkpoint.zookeeper.path'='/pcatalog/kafka_consumer/checkpoint/flights_hive',
        'gimel.kafka.message.value.type'='json',
        'gimel.kafka.throttle.batch.fetchRowsOnFirstRun'='10000000',
        'gimel.kafka.throttle.batch.maxRecordsPerPartition'='10000000',
        'gimel.kafka.whitelist.topics'='gimel.demo.flights.json',
        'gimel.kafka.zookeeper.connection.timeout.ms'='10000',
        'gimel.storage.type'='kafka',
        'key.deserializer'='org.apache.kafka.common.serialization.StringDeserializer',
        'key.serializer'='org.apache.kafka.common.serialization.StringSerializer',
        'numFiles'='0',
        'totalSize'='0',
        'transient_lastDdlTime'='1521993577',
        'value.deserializer'='org.apache.kafka.common.serialization.StringDeserializer',
        'value.serializer'='org.apache.kafka.common.serialization.StringSerializer');
        
   </td>
   </tr>
  </tbody>
</table>
_____________________________________________________
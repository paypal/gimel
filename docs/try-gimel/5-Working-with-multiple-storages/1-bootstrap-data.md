
* [Bootstrap Data](#bootstrap-data)
   * [Catalog Provider as USER](#catalog-provider-as-user)
   * [Bootstrap Flights Data](#bootstrap-flights-data)
      * [Create HDFS Datasets for loading Flights Data](#create-hdfs-datasets-for-loading-flights-data)
      * [Create Kafka Dataset](#create-kafka-dataset)
      * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
      * [Cache Kafka Data](#cache-kafka-data)
   * [Bootstrap Flights Lookup Data](#bootstrap-flights-lookup-data)
      * [Create HDFS Datasets for loading Flights Lookup Data](#create-hdfs-datasets-for-loading-flights-lookup-data)
      * [Create HBase Datasets](#create-hbase-datasets)
      * [Load Flights Lookup Data into HBase Datasets](#load-flights-lookup-data-into-hbase-datasets)
      * [Cache HBase Data](#cache-hbase-data)
   * [Cache Airports Data from HDFS](#cache-airports-data-from-hdfs)

# Bootstrap Data

## Catalog Provider as USER

```
gsql("set gimel.catalog.provider=USER");
```

## Bootstrap Flights Data

### Create HDFS Dataset for loading Flights Data

```
gsql("""set pcatalog.flights_hdfs.dataSetProperties={ 
    "datasetType": "HDFS",
    "fields": [],
    "partitionFields": [],
    "props": {
         "gimel.hdfs.data.format":"csv",
         "location":"hdfs://namenode:8020/flights/data",
         "datasetName":"pcatalog.flights_hdfs"
    }
}""")
```

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

### Load Flights Data into Kafka Dataset

```
gsql(
"insert into pcatalog.flights_kafka_json select * from pcatalog.flights_hdfs")
```

### Cache Kafka Data

```
gsql(
"cache table flights select * from  pcatalog.flights_kafka_json")
```

___________________________________________________________________________________________________________________

## Bootstrap Flights Lookup Data

### Create HDFS Datasets for loading Flights Lookup Data

```
gsql("""set pcatalog.flights_lookup_carrier_code_hdfs.dataSetProperties={ 
    "datasetType": "HDFS",
    "fields": [],
    "partitionFields": [],
    "props": {
         "gimel.hdfs.data.format":"csv",
         "location":"hdfs://namenode:8020/flights/lkp/carrier_code",
         "datasetName":"pcatalog.flights_lookup_carrier_code_hdfs"
    }
}""")
```

```
gsql("""set pcatalog.flights_lookup_airline_id_hdfs.dataSetProperties={ 
    "datasetType": "HDFS",
    "fields": [],
    "partitionFields": [],
    "props": {
         "gimel.hdfs.data.format":"csv",
         "location":"hdfs://namenode:8020/flights/lkp/airline_id",
         "datasetName":"pcatalog.flights_lookup_airline_id_hdfs"
    }
}""")
```

```
gsql("""set pcatalog.flights_lookup_cancellation_code_hdfs.dataSetProperties={ 
    "datasetType": "HDFS",
    "fields": [],
    "partitionFields": [],
    "props": {
         "gimel.hdfs.data.format":"csv",
         "location":"hdfs://namenode:8020/flights/lkp/cancellation_code",
         "datasetName":"pcatalog.flights_lookup_cancellation_code_hdfs"
    }
}""")
```

### Create HBase Datasets

```
gsql("""set pcatalog.flights_lookup_cancellation_code_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_cancellation_code",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_cancellation_code_hbase"
    }
}
""")
```

```
gsql("""set pcatalog.flights_lookup_carrier_code_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_carrier_code",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_carrier_code_hbase"
    }
}
""")
```

```
gsql("""set pcatalog.flights_lookup_airline_id_hbase.dataSetProperties=
{
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "Code",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "Description",
            "fieldType": "string",
            "isFieldNullable": false
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"Code",
        "gimel.hbase.table.name":"flights:flights_lookup_airline_id",
        "gimel.hbase.namespace.name":"flights",
        "gimel.hbase.columns.mapping":":key,flights:Description",
         "datasetName":"pcatalog.flights_lookup_airline_id_hbase"
    }
}
""")


```

### Load Flights Lookup Data into HBase Datasets

```
gsql(
"insert into pcatalog.flights_lookup_cancellation_code_hbase select * from pcatalog.flights_lookup_cancellation_code_hdfs")

gsql(
"insert into pcatalog.flights_lookup_airline_id_hbase select * from pcatalog.flights_lookup_airline_id_hdfs")

gsql(
"insert into pcatalog.flights_lookup_carrier_code_hbase select * from pcatalog.flights_lookup_carrier_code_hdfs")
```

### Cache lookup Tables from HBase

```
gsql("cache table lkp_carrier select * from pcatalog.flights_lookup_carrier_code_hbase")

gsql("cache table lkp_airline select * from pcatalog.flights_lookup_airline_id_hbase")

gsql("cache table lkp_cancellation select * from pcatalog.flights_lookup_cancellation_code_hbase")

```

___________________________________________________________________________________________________________________

## Cache Airports Data from HDFS

```
val sql="""cache table lkp_airport
select 
struct(lat,lon) as location
,concat(lat,",",lon) as location1
, * 
from 
(
select iata, lat, lon, country, city, name
, row_number() over (partition by iata order by 1 desc ) as rnk
from pcatalog.flights_lookup_airports_hdfs
) tbl
where rnk  = 1
"""

gsql(sql)
```

___________________________________________________________________________________________________________________
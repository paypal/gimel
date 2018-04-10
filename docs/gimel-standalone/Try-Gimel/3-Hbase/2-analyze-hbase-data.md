
* [G-SQL](#g--sql)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Kafka](#read-data-from-kafka)
* [Scala API](#scala-api)
    * [Set Options](#set-options)
    * [Load Flights Data into Kafka Dataset](#load-flights-data-into-kafka-dataset)
    * [Read Data from Kafka](#read-data-from-kafka)
   
# G-SQL


### Load Flights Lookup Data into HBase Datasets

```
gsql(
"insert into pcatalog.flights_lookup_cancellation_code_hbase select * from pcatalog.flights_lookup_cancellation_code_hdfs")

gsql(
"insert into pcatalog.flights_lookup_airline_id_hbase select * from pcatalog.flights_lookup_airline_id_hdfs")

gsql(
"insert into pcatalog.flights_lookup_carrier_code_hbase select * from pcatalog.flights_lookup_carrier_code_hdfs")
```

## Cache lookup Tables from HBase

```
gsql(
"cache table lkp_carrier select * from pcatalog.flights_lookup_carrier_code_hbase")

gsql(
"cache table lkp_airline select * from pcatalog.flights_lookup_airline_id_hbase")

gsql(
"cache table lkp_cancellation select * from pcatalog.flights_lookup_cancellation_code_hbase")

```

## Read Data from HBase
```
gsql("select * from lkp_carrier").show(10)
gsql("select * from lkp_airline").show(10)
gsql("select * from lkp_cancellation").show(10)
```
______________________________________________________

# Scala API

## Set options
```
scala> val datasetPropsJson = {
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
                              
scala> val datasetHivePropsJson = """{ 
                                         "datasetType": "HDFS",
                                         "fields": [],
                                         "partitionFields": [],
                                         "props": {
                                              "gimel.hdfs.data.format":"csv",
                                              "location":"hdfs://namenode:8020/flights/lkp/cancellation_code",
                                              "datasetName":"pcatalog.flights_lookup_cancellation_code_hdfs"
                                         }
                                     }"""
                                     
scala> val hbaseoptions = Map("pcatalog.flights_lookup_cancellation_code_hbase.dataSetProperties"->datasetPropsJson)

scala> val hiveOptions = Map("pcatalog.flights_lookup_cancellation_code_hdfs.dataSetProperties"->datasetHivePropsJson)
```

## Load Flights Data into HBase Dataset
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val hiveDf = dataSet.read("pcatalog.flights_lookup_cancellation_code_hdfs",hiveOptions)
scala> hiveDf.count
scala> val df =  dataSet.write("pcatalog.flights_lookup_cancellation_code_hbase",hivedf,hbaseoptions)
```

## Read Data from HBase
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val df = dataSet.read("pcatalog.flights_lookup_cancellation_code_hbase",hbaseoptions)
scala> df.show(10)
```
_________________________________________________
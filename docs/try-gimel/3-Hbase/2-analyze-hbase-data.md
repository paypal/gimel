
* [G-SQL](#g-sql)
    * [Load Flights Data into Hbase Dataset](#load-flights-data-into-hbase-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Hbase](#read-data-from-hbase)
* [Scala API for Catalog Provider-USER](#scala-api-for-catalog-provider-user)
    * [Set Options](#set-options)
    * [Load Flights Data into HBase Dataset](#load-flights-data-into-hbase-dataset-1)
    * [Read Data from HBase](#read-data-from-hbase-1)
* [Scala API for Catalog Provider-HIVE](#scala-api-for-catalog-provider-hive)
    * [Load Flights Data into HBase Dataset](#load-flights-data-into-hbase-dataset-2)
    * [Read Data from HBase](#read-data-from-hbase-2)
   
# G-SQL


### Load Flights Lookup Data into HBase Datasets

```
gsql("set gimel.hbase.rowkey=Code")
gsql("insert into pcatalog.flights_lookup_cancellation_code_hbase select * from pcatalog.flights_lookup_cancellation_code_hdfs")

gsql("set gimel.hbase.rowkey=Code")
gsql("insert into pcatalog.flights_lookup_airline_id_hbase select * from pcatalog.flights_lookup_airline_id_hdfs")

gsql("set gimel.hbase.rowkey=Code")
gsql("insert into pcatalog.flights_lookup_carrier_code_hbase select * from pcatalog.flights_lookup_carrier_code_hdfs")
```

## Cache lookup Tables from HBase

```
gsql("cache table lkp_carrier select * from pcatalog.flights_lookup_carrier_code_hbase")

gsql("cache table lkp_airline select * from pcatalog.flights_lookup_airline_id_hbase")

gsql("cache table lkp_cancellation select * from pcatalog.flights_lookup_cancellation_code_hbase")

```

## Read Data from HBase
```
gsql("select * from lkp_carrier").show(10)
gsql("select * from lkp_airline").show(10)
gsql("select * from lkp_cancellation").show(10)
```
______________________________________________________

# Scala API for Catalog Provider-USER

*Please execute the steps in this section if you have choosen CatalogProvider as USER or if you executed the following command*

```gsql("set gimel.catalog.provider=USER")```

## Set options
```
val datasetPropsJson = """{
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
                              }"""
                              
val datasetHivePropsJson = """{ 
                                         "datasetType": "HDFS",
                                         "fields": [],
                                         "partitionFields": [],
                                         "props": {
                                              "gimel.hdfs.data.format":"csv",
                                              "location":"hdfs://namenode:8020/flights/lkp/cancellation_code",
                                              "datasetName":"pcatalog.flights_lookup_cancellation_code_hdfs"
                                         }
                                     }"""
                                     
val hbaseoptions = Map("pcatalog.flights_lookup_cancellation_code_hbase.dataSetProperties"->datasetPropsJson)

val hiveOptions = Map("pcatalog.flights_lookup_cancellation_code_hdfs.dataSetProperties"->datasetHivePropsJson)
```

## Load Flights Data into HBase Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hiveDf = dataSet.read("pcatalog.flights_lookup_cancellation_code_hdfs",hiveOptions)
hiveDf.count
val df =  dataSet.write("pcatalog.flights_lookup_cancellation_code_hbase",hivedf,hbaseoptions)
```

## Read Data from HBase
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_lookup_cancellation_code_hbase",hbaseoptions)
df.show(10)
```
_________________________________________________


# Scala API for Catalog Provider-HIVE

*Please execute the steps in this section if you have choosen CatalogProvider as HIVE or if you executed the following command*

```gsql("set gimel.catalog.provider=HIVE")```

## Load Flights Data into HBase Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hiveDf = dataSet.read("pcatalog.flights_lookup_cancellation_code_hdfs")
hiveDf.count
val options = Map("gimel.hbase.rowkey"->"Code")
val df =  dataSet.write("pcatalog.flights_lookup_cancellation_code_hbase",hivedf,options)
```

## Read Data from HBase
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_lookup_cancellation_code_hbase")
df.show(10)
```

_________________________________________________

* [G-SQL](#g-sql)
    * [Load Flights Data into Elastic Dataset](#load-flights-data-into-elastic-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Elastic](#read-data-from-elastic)
* [Scala API for Catalog Provider-USER](#scala-api-for-catalog-provider--user)
    * [Set Options](#set-options)
    * [Load Flights Data into Elastic Dataset](#load-flights-data-into-elastic-dataset-1)
    * [Read Data from Elastic](#read-data-from-elastic-1)
* [Scala API for Catalog Provider-HIVE](#scala-api-for-catalog-provider--hive)
    * [Load Flights Data into Elastic Dataset](#load-flights-data-into-elastic-dataset-2)
    * [Read Data from Elastic](#read-data-from-elastic-2)
   
# G-SQL

## Load Flights Data into Elastic Dataset
```
gsql("insert into pcatalog.gimel_flights_elastic select * from pcatalog.flights_hdfs")
```

## Cache Flights 
```
gsql("cache table flights_elastic select * from  pcatalog.gimel_flights_elastic")
```

## Read Data from Elastic
```
gsql("select * from flights_elastic").show(10)
```
______________________________________________________

# Scala API for Catalog Provider-USER

*Please execute the steps in this section if you have choosen CatalogProvider as USER or if you executed the following command*

```gsql("set gimel.catalog.provider=USER")```
## Set options
```
val datasetPropsJson = """{
                                  "datasetType": "ELASTIC_SEARCH",
                                  "fields": [],
                                  "partitionFields": [],
                                  "props": {
                                		"es.mapping.date.rich":"true",
                                		"es.nodes":"http://elasticsearch",
                                		"es.port":"9200",
                                		"es.resource":"flights/data",
                                		"es.index.auto.create":"true",
                                		"gimel.es.schema.mapping":"{\"location\": { \"type\": \"geo_point\" } }",
                              		  "gimel.es.index.partition.delimiter":"-",
                              		  "gimel.es.index.partition.isEnabled":"true",
                              		  "gimel.es.index.read.all.partitions.isEnabled":"true",
                              		  "gimel.es.index.partition.suffix":"20180205",
                              		  "gimel.es.schema.mapping":"{\"executionStartTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\" }, \"createdTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"},\"endTime\": {\"format\": \"strict_date_optional_time||epoch_millis\", \"type\": \"date\"}}",
                              		  "gimel.storage.type":"ELASTIC_SEARCH",
                              		  "datasetName":"pcatalog.gimel_flights_elastic"
                                  }
                              }"""
val options = Map("pcatalog.gimel_flights_elastic.dataSetProperties"->datasetPropsJson)

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
```

## Load Flights Data into Elastic Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hivedf = dataSet.read("pcatalog.flights_hdfs",hiveOptions)
val df = dataSet.write("pcatalog.gimel_flights_elastic",hivedf,options)
df.count
```

## Read Data from Elastic
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.gimel_flights_elastic",options)
df.show(10)
```
_________________________________________________


# Scala API for Catalog Provider-HIVE

*Please execute the steps in this section if you have choosen CatalogProvider as HIVE or if you executed the following command*

```gsql("set gimel.catalog.provider=HIVE")```

## Load Flights Data into Elastic Dataset
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val hivedf = dataSet.read("pcatalog.flights_hdfs")
val df = dataSet.write("pcatalog.gimel_flights_elastic",hivedf)
df.count
```

## Read Data from Elastic
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.gimel_flights_elastic")
df.show(10)
```

_________________________________________________




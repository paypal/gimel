
* [G-SQL](#g--sql)
    * [Load Flights Data into Elastic Dataset](#load-flights-data-into-elastic-dataset)
    * [Cache Flights](#cache-flights)
    * [Read Data from Elastic](#read-data-from-elastic)
* [Scala API](#scala-api)
    * [Set Options](#set-options)
    * [Load Flights Data into Elastic Dataset](#load-flights-data-into-elastic-dataset)
    * [Read Data from Elastic](#read-data-from-elastic)
   
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

# Scala API

## Set options
```
scala> val datasetPropsJson = {
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
                              }
scala> val options = Map("pcatalog.flights_hdfs.dataSetProperties"->datasetPropsJson)
```

## Load Flights Data into Elastic Dataset
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val hivedf = dataSet.read("pcatalog.flights_hdfs",options)
scala> val df = dataSet.write("pcatalog.gimel_flights_elastic",hivedf,options)
scala> df.count
```

## Read Data from Elastic
```
scala> import com.paypal.gimel._
scala> val dataSet = DataSet(spark)
scala> val df = dataSet.read("pcatalog.gimel_flights_elastic",options)
scala> df.show(10)
```
_________________________________________________




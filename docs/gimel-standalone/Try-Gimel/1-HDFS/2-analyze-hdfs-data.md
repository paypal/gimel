
* [G-SQL](#g-sql)
    * [Cache Airports Data from HDFS](#cache-airports-data-from-hdfs)
    * [Read Data from HDFS](#read-data-from-hdfs)
    * [Count Records from HDFS](#count-records-from-hdfs)
* [Scala API](#scala-api)
    * [Read Data from HDFS](#read-data-from-hdfs-1)
    * [Count Records from HDFS](#count-records-from-hdfs-1)
   
# G-SQL

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
```
```
gsql(sql)
```
## Read Data from HDFS
```
gsql("select * from pcatalog.flights_hdfs").show()
```

## Count Records From HDFS
```
gsql("select * from pcatalog.flights_hdfs").count()
```
______________________________________________________

# Scala API

## Read Data from HDFS
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
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
val options=  Map("pcatalog.flights_hdfs.dataSetProperties"->datasetHivePropsJson)
val df = dataSet.read("pcatalog.flights_hdfs",options)
df.count
```

## Count Records From HDFS
```
import com.paypal.gimel._
val dataSet = DataSet(spark)
val df = dataSet.read("pcatalog.flights_hdfs")
df.count
```
_________________________________________________




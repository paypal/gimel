* [Elastic Search API](#elastic-search-api)
  * [Note](#note)
  * [Create Hive Table](#create-hive-table-pointing-to-elastic-search)
  * [Common Imports](#common-imports-in-all-es-api-usages)
  * [Elastic Search Write API](#write-api-es)
    * [Prepare Test Data](#prepare-test-data-for-write-es)
    * [Write Data](#write-data-api-elastic-search)
    * [Write Data into Partitioned Index](#write-data-api-elastic-search-for-partitioned-index)
  * [Elastic Search Read API](#read-api-es)
    * [Read Data - Simple](#read-data-from-es)
    * [Read Data - Multiple indexes](#read-data-from-es-for-multiple-indexes)
    * [Read Data - Wildcard Match](#read-data-from-es-using-wildcard)

--------------------------------------------------------------------------------------------------------------------


# Elastic Search API

## Note

* Under the hood, we are leveraging the [Elastic Search Spark Connector](https://www.elastic.co/guide/en/elasticsearch/hadoop/5.4/spark.html)

--------------------------------------------------------------------------------------------------------------------

## Create Hive table pointing to Elastic Search

The following hive table points to an elastic search Index

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS pcatalog.sampleESTable
(
  `data` string COMMENT 'from deserializer'
)
LOCATION '/tmp/pcatalog/sampleESTable'
TBLPROPERTIES (
  'gimel.storage.type' = 'ELASTICSEARCH',
  'es.index.auto.create'='true',
  'es.nodes'='http://es_node',
  'es.port'='8080',
  'es.resource'='sampleESTable_index/data'
)
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| es.nodes | Y | ES host | Elastic search host | http://localhost | |
| es.port | Y | ES Port | the ES service port |8080 | |
| es.resource | Y| the index and type | sampleESTable_index/data | |

--------------------------------------------------------------------------------------------------------------------



## Common Imports in all ES API Usages

```scala
import com.paypal.gimel._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

import spray.json._
import spray.json.DefaultJsonProtocol._
```

--------------------------------------------------------------------------------------------------------------------


## Write Api ES

### Prepare Test Data for Write ES

```scala
def stringed(n: Int) = s"""{"id": ${n},"name": "MAC-${n}", "address": "MAC-${n+1}", "age": "${n+1}", "company": "MAC-${n}", "designation": "MAC-${n}", "salary": "${n * 10000}" }"""
val numberOfRows=10000
val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val dataFrameToWrite: DataFrame = sparkSession.read.json(rdd)
dataFrameToWrite.show
```

### Write Data Api Elastic Search

```scala
val dataSet: DataSet = DataSet(sparkSession)
val dataFrameWritten = dataSet.write("pcatalog.sampleESTable",dataFrameToWrite)

```

### Write Data Api Elastic Search for Partitioned Index

```scala
val dataSet = DataSet(sc)
val options: Map[String, String] = Map("gimel.es.index.partition.suffix"->"20170602")
val json3 = s"""{"name" : "abcd", "age" : "28","gender":"m"}"""
val json4 = s"""{"name" : "efgh", "age" : "28","gender":"m"}"""
val rdd1 = sc.parallelize(Seq(json3,json4))
val df1 = sparkSession.read.json(rdd1)
val res = dataSet.write("pcatalog.sampleESTable",df1,options)
```


--------------------------------------------------------------------------------------------------------------------


## Read Api ES

### Read Data from ES

```scala
val dataFrameRead = dataSet.read("pcatalog.sampleESTable")
dataFrameRead.show
```

### Read Data from ES for Multiple Indexes
```scala
val options: Map[String, String] = Map("gimel.es.index.partition.isEnabled"->"true","gimel.es.index.partition.delimiter"->"_","gimel.es.index.partition.suffix"->"20170602,20170603")
val res1 = dataSet.read("pcatalog.sampleESTable",options)
res1.show
```

### Read Data from ES using wildcard
```scala
val options: Map[String, String] = Map("gimel.es.index.partition.isEnabled"->"true","gimel.es.index.partition.delimiter"->"_","gimel.es.index.partition.suffix"->"*","gimel.es.index.read.all.partitions.isEnabled"->"true")
val res1 = dataSet.read("pcatalog.sampleESTable",options)
res1.show
```
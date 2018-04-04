
* [Hive API](#hive-api)
  * [Create Hive Table](#create-hive-table-for-read-and-write)
  * [Common Imports](#common-imports-in-all-hive-api-usages)
  * [Hive Write API](#write-api-hive)
    * [Prepare Test Data](#prepare-test-data-for-write-hive)
    * [Write Data](#write-data-api-hive)
  * [Hive Read API](#read-api-hive)
    * [Read Data](#read-data-from-hive)

--------------------------------------------------------------------------------------------------------------------


# Hive API

## Create Hive table for Read and Write

```sql
CREATE external TABLE IF NOT EXISTS pcatalog.pc_sampleHiveTable
(
id string,
name string,
rev string

)
STORED AS parquet
LOCATION '/tmp/pcatalog/pc_sampleHiveTable'
```

--------------------------------------------------------------------------------------------------------------------


## Common Imports in all Hive API Usages

```scala
import com.paypal.gimel._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import spray.json._
import spray.json.DefaultJsonProtocol._
```

--------------------------------------------------------------------------------------------------------------------


## Write Api Hive

### Prepare Test Data for Write Hive

```scala
def stringed(n: Int) = s"""{"id": ${n},"name": "MAC-${n}", "address": "MAC-${n+1}", "age": "${n+1}", "company": "MAC-${n}", "designation": "MAC-${n}", "salary": "${n * 10000}" }"""
val numberOfRows=10000
val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val dataFrameToWrite: DataFrame = sparkSession.read.json(rdd)
dataFrameToWrite.show
```


### Write Data Api Hive

```scala
//Initiate DataSet
val dataSet = DataSet(sparkSession)
//options
val options = "gimel.kafka.throttle.batch.fetchRowsOnFirstRun=1000000:gimel.kafka.throttle.batch.maxRecordsPerPartition=1000000"
//write
val dataFrameWritten = dataSet.write("pcatalog.pc_sampleHiveTable",dataFrameToWrite,options)

```

--------------------------------------------------------------------------------------------------------------------


## Read Api Hive

### Read Data from Hive

```scala
val dataFrameRead = dataSet.read("pcatalog.pc_sampleHiveTable")
dataFrameRead.show
```

--------------------------------------------------------------------------------------------------------------------

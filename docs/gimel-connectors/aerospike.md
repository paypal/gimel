
* [Aerospike API](#aerospike-api)
  * [Note](#note)
  * [Design Considerations](#design-considerations)
  * [Create Aerospike Table](#create-aerospike-table)
  * [Common Aerospike Hive Table](#create-hive-table-pointing-to-aerospike-set)
  * [Common imports](#common-imports-in-all-aerospike-api-usages)
  * [Sample API usage](#aerospike-api-usage)


--------------------------------------------------------------------------------------------------------------------


# Aerospike API

## Note

* Experimental API, meaning there is no production usecase on this yet.
* Basic Benchmarking has been performed to validate throughputs

--------------------------------------------------------------------------------------------------------------------


## Design Considerations

### Reads

* Aerospike Spark Connector(Aerospark) is used to read data directly into a DataFrame (https://github.com/sasha-polev/aerospark)
* The reason for choosing Aerospark over Java client for read is explained below:
    * Aerospark : Read data directly into a DataFrame
    * Native Java Client : Driver program has to first collect all records from the aerospike nodes and then create a dataFrame which is a costly operation.

### Writes

* Aerospike Native Java Client Put API is used.
* For each partition of the Dataframe a client connection is established, to write data from that partition to Aerospike.

--------------------------------------------------------------------------------------------------------------------

## Create Hive Table pointing to Aerospike Set

The following hive table points to a Aerospike

```sql
CREATE EXTERNAL TABLE `pcatalog.test_aero`(
  `payload` string)
LOCATION
  'hdfs://tmp/test_aero'
TBLPROPERTIES (
  'gimel.aerospike.namespace'='test',
  'gimel.aerospike.port'='3000',
  'gimel.aerospike.seedhost'='hostname',
  'gimel.aerospike.set'='test_aero_connector',
  'gimel.storage.type'='AEROSPIKE')
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.aerospike.seed.hosts | Y | list of hosts fqdn or ip | localhost | |
| gimel.aerospike.port | Y | port | 3000 | 3000 |
| gimel.aerospike.namespace | Y | the namespace in aerospike | test |  |
| gimel.aerospike.set | Y | aerospike set name | sample_set |  |
| gimel.aerospike.rowkey | N | the row key for the set | col1 | first column in dataframe |


--------------------------------------------------------------------------------------------------------------------


## Common Imports in all Aerospike API Usages

```scala
import com.paypal.gimel._
import org.apache.spark._
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql._
import spray.json._;
import spray.json.DefaultJsonProtocol._;

```


--------------------------------------------------------------------------------------------------------------------


## Aerospike API Usage

```scala

// READ

val dataSet = DataSet(sparkSession);
val df = dataSet.read("pcatalog.test_aero")

// WRITE

// Create mock data
def stringed(n: Int) = s"""{"id": ${n},"name": "MAC-${n}", "address": "MAC-${n+1}", "age": "${n+1}", "company": "MAC-${n}", "designation": "MAC-${n}", "salary": "${n * 10000}" }""";
val numberOfRows=20;
val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }.toSeq;
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts);
val dataFrameToWrite: DataFrame = sparkSession.read.json(rdd);
dataFrameToWrite.show;

// write it to Aerospike via PCatalog
val df = dataSet.write("pcatalog.test_aero",dataFrameToWrite)

```

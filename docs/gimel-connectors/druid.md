* [Druid API](#druid-api)
  * [Note](#note)
  * [Create Druid Table](#create-druid-table)
  * [Common Druid Hive Table](#create-hive-table)
  * [Catalog Properties](#catalog-properties)
  * [Common imports](#common-imports)
  * [Sample API usage](#cassandra-api-usage)


--------------------------------------------------------------------------------------------------------------------

# Druid API

## Note

* Supports only Writes in real-time ingestion mode.
* Under the hood - the [druid-io/tranquility](https://github.com/druid-io/tranquility) connector is used for spark-realtime ingestion.
* Reads - Not implemented yet

--------------------------------------------------------------------------------------------------------------------

## Create Hive Table

The following hive table points to a Druid

```sql
CREATE EXTERNAL TABLE `pcatalog.druid_testing`(
  `id` string,
  `type` string,
  `time_updated` string
)
LOCATION
  'hdfs:///tmp/gimel/pcatalog.druid_testing'
TBLPROPERTIES (
  'gimel.storage.type'='druid',
  'gimel.druid.zookeeper.hosts' = 'druid_zk_host_1:2181,druid_zk_host_1:2181,druid_zk_host_1:2181',
  'gimel.druid.cluster.index.service' = 'druid/overlord',
  'gimel.druid.cluster.discovery.path' = '/druid/discovery',
  'gimel.druid.datasource.name' = 'default_datasource',
  'gimel.druid.datasource.dimensions' = '["type", "time_updated"]',
  'gimel.druid.datasource.metrics' = '[{"type", "hyperUnique", "field_name", "id", "name", "distinct_id"}]',
  'gimel.druid.timestamp.fieldname' = 'time_updated',
  'gimel.druid.timestamp.format' = 'seconds'
)
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties

Refer [Tranquility documentation](https://github.com/druid-io/tranquility/blob/master/docs/spark.md) for under the hood configurations


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.druid.zookeeper.hosts		| Y | The Host Name List for ZK  | druid_zk_host_1:2181,druid_zk_host_2:2181 | |
| gimel.druid.cluster.index.service	| Y | Index Name in Druid  | druid/overlord | |
| gimel.druid.cluster.discovery.path| Y | The Discovery Path in Druid Cluster  | /druid/discovery | |
| gimel.druid.datasource.name		| Y | The data source name | default_datasource | |
| gimel.druid.datasource.dimensions	| Y | The list of Dimensions specified in JSON format  | ["type", "time_updated"] | |
| gimel.druid.datasource.metrics	| Y | The list of metrics in Druid Index  | [{"type", "hyperUnique", "field_name", "id", "name", "distinct_id"}] | |
| gimel.druid.timestamp.fieldname	| Y | The Time Field  | time_updated | |
| gimel.druid.timestamp.format		| Y | Format of the Time Field  | seconds | |

--------------------------------------------------------------------------------------------------------------------


## Common Imports

```scala
import com.paypal.gimel._
import org.apache.spark._
import org.apache.spark.sql._

```

--------------------------------------------------------------------------------------------------------------------


## Druid API Usage

```scala
val dataSet= DataSet(sparkSession)

val map = List(Map("id" -> 1, "type" -> "U", "time_updated" -> "10000000"))
val rdd = sc.parallelize(map)

val dataSetProps = Map("load_type" -> "realtime")
dataSet.write("pcatalog.sampleDruidData", rdd, dataSetProps)
```

--------------------------------------------------------------------------------------------------------------------

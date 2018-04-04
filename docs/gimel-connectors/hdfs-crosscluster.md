* [Cross Cluster HDFS API](#cross-cluster-api)
   * [Note](#note)
   * [Limitations](#limitations)
   * [Create Hive Table](#create-hive-table)
   * [Catalog Properties](#catalog-properties)
   * [Common imports](#common-imports)
   * [API Usage](#api-usage)

--------------------------------------------------------------------------------------------------------------------

# Cross Cluster HDFS API

## Note

### How does it work?

Cross Cluster API is nothing but HDFS API that can read across clusters even if they are kerberized.

For Cross Cluster read/write to work -
    While starting the spark application - add the namenodes of all the clusters that you are going to work with.
    Pass the following spark conf while starting the spark app. Example for working with several clusters.
    ```bash
    "spark.yarn.access.namenodes":"hdfs://cluster_2_name_node:8020/,hdfs://cluster_2_name_node:8020/"
    ```

Alluxio -
    The Same API works with Alluxio as well, provided the property `gimel.hdfs.nn` starts with string `alluxio`.

Under the hood, we are using the spark read API.
Hence where ever applicable : a filter on a partitioned column will result in pruning.


### Limitations

Cross Cluster Reads have potential pitfalls too : if the source path has terabytes of data & there is no pruning of partitions / paths -
this will lead to long running job that attempts to transfer TeraBytes of data across cluster. Cross cluster API is not meant for this type of usecase.

### Preemptive Guard

With above limitation in consideration - we have a guard that users can leverage when they are not sure about the volume that is going to be read from the source cluster.
Property `gimel.hdfs.data.crosscluster.threshold`, when set to  say `250` - will consider the read-size-threshold to be `250GB`.
Preemptively - before the API reads source path : a check on the source path's size will be compared against `250GB`.
If size  > `250GB` - an exception with be thrown that threshold is exceeded.

--------------------------------------------------------------------------------------------------------------------


## Create hive table
```
CREATE EXTERNAL TABLE `pcatalog.gimel_xcluster_pi`(
  `payload` string)
LOCATION
  'hdfs:///tmp/pcatalog/xcluster_remote_hadoop_cluster_text_smoke_test'
TBLPROPERTIES (
  'gimel.hdfs.data.format'='text',
  'gimel.hdfs.data.location'='/tmp/examples/pi.py',
  'gimel.hdfs.nn'='hdfs://remote_hadoop_cluster_namenode:8020',
  'gimel.hdfs.storage.name'='remote_hadoop_cluster',
  'gimel.storage.type'='HDFS'
  )
```

___________________________________________________________________________________________________________________


## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.hdfs.data.format | Y | Format of the data | <br>text<br>parquet<br>csv<br>orc<br> | text |
| gimel.hdfs.data.location | Y | HDFS location | /tmp/examples/pi.py |  |
| gimel.hdfs.nn | Y | Name Notde URI | <br>hdfs://remote_hadoop_cluster_name_node:8020<br>alluxio://namenode:19998<br> | the namenode of cluster where spark application is running currently|
| gimel.hdfs.storage.name | Y | Name of cluster | <br>remote_hadoop_cluster<br>remote_hadoop_cluster11<br>alluxio<br> | |


--------------------------------------------------------------------------------------------------------------------

## Common Imports
```scala
import com.paypal.gimel._
import org.apache.spark.sql._
```

--------------------------------------------------------------------------------------------------------------------

## API Usage

### Read from different Cluster
```scala
//Initiate DataSet
val dataset = DataSet(sparkSession)
//DataSet Name
///Options set by default
///Maximum Threshold data to query is 50GB by default
///To increase Threshold
val options = Map("gimel.hdfs.data.crosscluster.threshold" -> "250");
//Read some data
dataset.read("pcatalog.gimel_xcluster_pi",options)
```

### SQL Support
```scala
sparkSession.sql("set gimel.hdfs.data.crosscluster.threshold=250");
val data = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch("select * from pcatalog.gimel_xcluster_pi",sparkSession);
data.show
```

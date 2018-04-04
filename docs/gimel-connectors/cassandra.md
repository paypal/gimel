
* [Cassandra API](#cassandra-api)
  * [Note](#note)
  * [Create Cassandra Table](#create-cassandra-table)
  * [Common Cassandra Hive Table](#create-hive-table-pointing-to-cassandra-table)
  * [Common imports](#common-imports-in-all-cassandra-api-usages)
  * [Sample API usage](#cassandra-api-usage)


--------------------------------------------------------------------------------------------------------------------



# Cassandra API

## Note

* Experimental API, meaning there is no production usecase on this yet.

--------------------------------------------------------------------------------------------------------------------

## Design Considerations

* Under the hood - we are leveraging the [DataStax spark connector for Cassandra](https://github.com/datastax/spark-cassandra-connector).

--------------------------------------------------------------------------------------------------------------------


## Create Cassandra Table


```sql
CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};

CREATE TABLE test_table_1 (
name text,
age int,
rev int,
PRIMARY KEY (name)
);

insert into test_table_1 (name,age,rev) values ( 'deepak',100,9999);
```

--------------------------------------------------------------------------------------------------------------------

## Create Hive Table pointing to Cassandra table

The following hive table points to a Cassandra

```sql
create table pcatalog.cassandra_testing (
payload string
)
location 'hdfs:///tmp/cassandra_testing'
TBLPROPERTIES (
'cassandra.connection.host'='hostname',
'gimel.storage.type'='CASSANDRA',
'gimel.cassandra.cluster.name'='POC_Cluster',
'gimel.cassandra.keyspace.name'='test',
'gimel.cassandra.table.name'='test_table_1',
'gimel.cassandra.pushdown.is.enabled'='true',
'gimel.cassandra.table.confirm.truncate'='false',
'spark.cassandra.input.split.size_in_mb'='128'
)
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties

Refer [DataStax Cassandra Connector FAQ](https://github.com/datastax/spark-cassandra-connector/blob/master/doc/FAQ.md) for indepth details.

| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| spark.cassandra.connection.host | Y | Host fqdn or IP | localhost | |
| gimel.cassandra.cluster.name | Y | Name of cluster | poc | |
| gimel.cassandra.keyspace.name | Y | Key Space in Cassandra | test | |
| gimel.cassandra.table.name | Y | Cassandra Table Name | test_table_1 |  |
| gimel.cassandra.pushdown.is.enabled | N | Setting to enable pushdown | true | true |
| spark.cassandra.input.split.size_in_mb | N | <br>The number of Spark partitions(tasks) created is directly controlled by the setting spark.cassandra.input.split.size_in_mb.<br>This number reflects the approximate amount of Cassandra Data in any given Spark partition<br> | 128 | 48 |


--------------------------------------------------------------------------------------------------------------------



## Common Imports in all Cassandra API Usages

```scala
import org.apache.spark.sql._;
import com.paypal.gimel.logger.Logger;
import com.paypal.gimel._;

```


## Cassandra API Usage

```scala

val dataSet= DataSet(sparkSession)

sparkSession.setConf("spark.cassandra.connection.host", "hostname_or_ip")
sparkSession.setConf("spark.cleaner.ttl", "3600")

val cassandraDfOptions= scala.collection.immutable.Map(
"keyspace" -> "test", "table" -> "test_table_1"
)

val fromCassandra1 = dataSet.read("pcatalog.cassandra_testing")
val fromCassandra2 = sqlContext.read.format("org.apache.spark.sql.cassandra").options(cassandraDfOptions).load()
fromCassandra2.show
```

--------------------------------------------------------------------------------------------------------------------


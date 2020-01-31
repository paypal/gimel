* [HBASE API](#hbase-api)
  * [Overview](#overview)
  * [Catalog Providers](#catalog-providers)
  * [Catalog Properties](#catalog-properties)
  * [Common Imports](#common-imports-in-all-hbase-api-usages)
  * [HBASE Write API (via SHC Connector)](#hbase-write-api-via-shc-connector)
    * [Prepare Test Data](#prepare-test-data-for-write)
    * [Write all columns in Dataframe to HBase table](#write-all-columns)
    * [Write specific columns (given in input option gimel.hbase.columns.mapping by user) only to HBase table](#write-specific-columns)
    * [Write through GSQL](#write-through-gsql)
  * [HBASE Read API (via SHC Connector)](#hbase-read-api-via-shc-connector)
    * [Read all columns from HBase table](#read-all-columns)
    * [Read specific columns(given in input option hbase.columns.mapping by user) from HBase table](#read-specific-columns)
    * [Read same column name present in different column family](#read-same-column-name-present-in-different-column-family)
    * [Read Records with limit](#read-records-with-limit)
    * [Read through GSQL](#read-through-gsql)
        * [Read all rows](#read-through-gsql)
        * [Push Down Filters](#push-down-filters)
            * [Lookup by rowKey via SHC](#lookup-by-rowkey-via-shc)
            * [Lookup by rowKey Prefix via SHC](#lookup-by-rowkey-prefix-via-shc)
            * [Lookup by column via SHC](#lookup-by-column-via-shc)
            * [Limit records](#limit-records)
  * [HBASE Lookup via Java Get API](#hbase-lookup-via-java-get-api)
    * [Lookup by RowKey](#lookup-by-rowkey)
    * [Lookup by RowKey and ColumnFamily](#lookup-by-rowkey-and-columnfamily)


--------------------------------------------------------------------------------------------------------------------


# HBASE API

## Overview

* This API is meant to read/write from/to HBase.
* Under the hood, we are leveraging open source [hortonworks-spark connector](https://github.com/hortonworks-spark/shc).

### Limitations
* Read/Write API : Doesn't support multi-column ROWKEY
* Read/Write API : Performance is bounded on how HBase table is created

### Design Considerations

Following are the advantages of using SHC
* This provides support for spark hbase integration on Dataframe and Dataset level.
* Writes are converted to HBase Puts for each partition. [code](https://github.com/hortonworks-spark/shc/blob/master/core/src/main/scala/org/apache/spark/sql/execution/datasources/hbase/HBaseRelation.scala)
* It supports the pushdown of spark filters to HBase filters.

Following are the limitations if one writes own write implementations via JAVA Client for HBASE
* Scalability Challenges - We have to sequentially run all the puts in driver which takes a lot of time even for small amount of data (>40,000)
* Java Put Object is not Serializable.

___________________________________________________________________________________________________________________

## Catalog Providers

### Dynamic Dataset: You can provide the dataset name in format Hbase.CLusterName.NamespaceName.TableName
Example: <br>
Dataset name: Hbase.Test_Cluster.default.test_table<br>
It will take the namespace and table name from the dataset name at runtime.
Here the namespace name is default and hbase table name is test_table.
You will need to provide the following mandatory properties
- gimel.hbase.rowkey in case of write


### HIVE - Create Hive Table pointing to HBase table

The following hive table points to a hbase table named default:test_emp with column families personal and professional

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `default.hbase_cluster_default_test_emp`(
  `payload` string
  )
TBLPROPERTIES (
  'gimel.hbase.columns.mapping'='personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary',
  'gimel.hbase.table.name'='test_emp',
  "gimel.hbase.namespace.name"="default",
  'gimel.storage.type'='HBASE'
  )
```

### USER

This is mainly used for debugging purposes. 

```scala
val dataSetProperties ="""
{
    "datasetType": "HBASE",
    "fields": [],
    "partitionFields": [],
    "props": {
            "gimel.storage.type":"HBASE",
            "gimel.hbase.columns.mapping":":key,personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary",
            "gimel.hbase.namespace.name":"default",
            "gimel.hbase.table.name":"test_emp",
            "datasetName":"udc.default_test_emp"
       }
}

gsql(s"""set udc.default_test_emp.dataSetProperties=$dataSetProperties""")

// or 

val options = Map("udc.default_test_emp.dataSetProperties" -> dataSetProperties)

"""

```
___________________________________________________________________________________________________________________


## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.hbase.table.name | Y | HBASE Table Name | tableName or namespaceName:tableName | |
| gimel.hbase.namespace.name | Y | HBASE Name Space | default | default |
| gimel.hbase.columns.mapping | Y | Mapping of column family to column | :key,cols:column2,cols:column3 | while writes, this is taken implicitly from DataFrame |
| gimel.hbase.rowkey | Y | Mandataory only for write API | id | rowkey|
| gimel.hbase.colName.with.cfName.appended | N | <br>Used for appending the column family to column name in output dataframe. | true/false | false |

--------------------------------------------------------------------------------------------------------------------

## Common Imports in all Hbase API Usages

```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
```
___________________________________________________________________________________________________________________


## HBASE Write API (via SHC Connector)

### Prepare Test Data for write

```scala
def stringed(n: Int) = s"""{"id": ${n},"name": "MAC-${n}", "address": "MAC-${n+1}", "age": "${n+1}", "company": "MAC-${n}", "designation": "MAC-${n}", "salary": "${n * 10000}" }"""
val numberOfRows=10000
val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = spark.sparkContext.parallelize(texts)
val dataFrameToWrite: DataFrame = spark.read.json(rdd)
dataFrameToWrite.show
```

### Write all columns

```scala
// Write all columns in Dataframe to HBase table

val dataSet = com.paypal.gimel.DataSet(sparkSession)
val options: Map[String,Any] = Map("gimel.hbase.rowkey" -> "id", "gimel.hbase.columns.mapping" -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
val dataFrameWritten = dataSet.write("udc.Hbase.ClusterName.default.test_emp", dataFrameToWrite, options)
```

### Write specific columns

```scala
// Write specific columns to HBase table

val options: Map[String,Any] = Map("gimel.hbase.rowkey" -> "id", "gimel.hbase.columns.mapping" -> "personal:age")
val dataFrameWritten = dataSet.write("udc.Hbase.ClusterName.default.test_emp", dataFrameToWrite, options)

```

### Write through GSQL

#### Insert into HBase from ES

```scala
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._
// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")

spark.sql("set gimel.hbase.rowkey=flight_number")
spark.sql("set gimel.hbase.columns.mapping=cf1:air_time,cf1:airline_delay,cf2:airline_name")
val df = gsql("insert into udc.Hbase.ClusterName.default.test_emp select * from udc.Elastic.Gimel_Dev.default.gimel_tau_flights")
```
___________________________________________________________________________________________________________________

## HBase Read API (via SHC Connector)

### Read all columns

```scala
// Read all columns from HBase table
val options: Map[String,Any] = Map("gimel.hbase.rowkey" -> "id", "gimel.hbase.columns.mapping" -> "personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary")
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

### Read specific columns

```scala
// Read specific columns(given in input option gimel.hbase.columns.mapping by user) from HBase table

val options: Map[String,Any] = Map("gimel.hbase.rowkey" -> "id", "gimel.hbase.columns.mapping" -> "personal:name,professional:salary")
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

### Read same column name present in different column family

In this case, column family name will be appended with column name.

```scala
val options: Map[String,Any] = Map("gimel.hbase.colName.with.cfName.appended" -> "true", "gimel.hbase.rowkey" -> "id", "gimel.hbase.columns.mapping" -> "personal:name,professional:name")
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

##### Read records with limit

```scala
val options: Map[String,Any] = Map("gimel.hbase.rowkey" -> "id", 
  "gimel.hbase.columns.mapping" -> "personal:name,professional:name", 
  "spark.hbase.connector.pageSize" -> 10)
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

Limit is pushed down to HBase with "spark.hbase.connector.pageSize" -> 10 property. PageFilter will be set in open source SHC connector.

### Read through GSQL

```scala
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._
// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")
```

#### Read all rows

```scala
gsql("set gimel.hbase.columns.mapping=personal:name,personal:age")
val df = gsql("select * from udc.Hbase.ClusterName.default.test_emp")
df.show
```

#### Push Down Filters

##### Lookup by rowKey via SHC

```scala
gsql("set gimel.hbase.columns.mapping=personal:name,personal:age")
val df = gsql("select * from udc.Hbase.ClusterName.default.test_emp where rowkey='1-MAC'")
df.show

df.explain
== Physical Plan ==
*(1) Filter isnotnull(rowkey#181)
+- *(1) Scan HBaseRelation(Map(catalog -> {"table":{"namespace":"default", "name":"test_emp", "tableCoder":"PrimitiveType"},
"rowkey":"rowkey",
"columns":{
"rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string", "length":"50"},
"name":{"cf":"personal", "col":"name", "type":"string"},
"age":{"cf":"personal", "col":"age", "type":"string"}
}
}
    ),None) [rowkey#181,name#182,age#183] PushedFilters: [IsNotNull(rowkey), *EqualTo(rowkey,1-MAC)], ReadSchema: struct<rowkey:string,name:string,age:string>
```

Here spark filter "EqualTo" is pushed down to hbase through SHC connector.

##### Lookup by rowKey Prefix via SHC

```scala
gsql("set gimel.hbase.columns.mapping=personal:name,personal:age")
val df = gsql("select * from udc.Hbase.ClusterName.default.test_emp where rowkey like '1%'")

df.explain
== Physical Plan ==
*(1) Filter (isnotnull(rowkey#131) && StartsWith(rowkey#131, 1))
+- *(1) Scan HBaseRelation(Map(catalog -> {"table":{"namespace":"default", "name":"test_emp", "tableCoder":"PrimitiveType"},
"rowkey":"rowkey",
"columns":{
"rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string", "length":"50"},
"name":{"cf":"personal", "col":"name", "type":"string"},
"age":{"cf":"personal", "col":"age", "type":"string"}
}
}
    ),None) [rowkey#131,name#132,age#133] PushedFilters: [IsNotNull(rowkey), StringStartsWith(rowkey,1)], ReadSchema: struct<rowkey:string,name:string,age:string>
```

Here spark filter "StringStartsWith" is pushed down to hbase filter "PrefixFilter".

##### Lookup by column via SHC

```scala
gsql("set gimel.hbase.columns.mapping=personal:name,personal:age")
val df = gsql("select * from udc.Hbase.ClusterName.default.test_emp where age='2'")

df.explain
== Physical Plan ==
*(1) Filter isnotnull(age#537)
+- *(1) Scan HBaseRelation(Map(catalog -> {"table":{"namespace":"default", "name":"test_emp", "tableCoder":"PrimitiveType"},
"rowkey":"rowkey",
"columns":{
"rowkey":{"cf":"rowkey", "col":"rowkey", "type":"string", "length":"50"},
"name":{"cf":"personal", "col":"name", "type":"string"},
"age":{"cf":"personal", "col":"age", "type":"string"}
}
}
    ),None) [rowkey#535,name#536,age#537] PushedFilters: [IsNotNull(age), *EqualTo(age,2)], ReadSchema: struct<rowkey:string,name:string,age:string>
```

Here spark filter "EqualTo" is pushed down to hbase filter "SingleColumnValueFilter".

*Note: All these push downs are verified through "Number of Requests" stats on Hbase master UI*

##### Limit Records

```scala
gsql("set gimel.hbase.columns.mapping=personal:name,personal:age")
val df = gsql("select * from udc.Hbase.ClusterName.default.test_emp limit 10")

```

Limit is pushed down to HBase. This is done by setting the PageFilter in open source SHC connector.
___________________________________________________________________________________________________________________

## HBase Lookup (via Java Get API)

### Lookup by rowKey

```scala
//Get all columns of all column families in a row
val options: Map[String,Any] = Map("gimel.hbase.operation" -> "get", "gimel.hbase.get.filter" -> "rowKey=1")
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

### Lookup by rowKey and ColumnFamily

```scala
//Get all columns in a column family
val options: Map[String,Any] = Map("gimel.hbase.operation" -> "get", "gimel.hbase.get.filter" -> "rowKey=1:toGet=personal")
val dataFrameRead = dataSet.read("udc.Hbase.ClusterName.default.test_emp", options)
dataFrameRead.show
```

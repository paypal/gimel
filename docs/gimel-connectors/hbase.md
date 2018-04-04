* [HBASE API](#hbase-api)
  * [Note](#note)
  * [Create Hive Table](#create-hive-table-pointing-to-hbase-table)
  * [Common Imports](#common-imports-in-all-hbase-api-usages)
  * [HBASE Write API - Put](#hbase-write-api-put)
    * [Prepare Test Data](#prepare-test-data-for-write)
    * [Write all columns(also present in ddl) in Dataframe to HBase table](#write-all-columns)
    * [Write new columns(not in ddl) along with other columns in dataframe to HBase table](#write-new-columns)
    * [Write specific columns (given in input option hbase.columns.mapping by user) only to HBase table](#write-specific-columns)
  * [HBASE Read API - Scan/BulkGet](#hbase-read-api-scan)
    * [Read all columns from HBase table](#read-all-columns)
    * [Read specific columns(given in input option hbase.columns.mapping by user) from HBase table](#read-specific-columns)
  * [HBASE Read API - Lookup/Get](#hbase-read-api-lookup)
    * [Lookup by RowKey](#lookup-by-rowkey)
    * [Lookup by RowKey and ColumnFamily](#lookup-by-rowkey-and-columnfamily)
    * [Lookup by RowKey, ColumnFamily and Column](#lookup-by-rowkey-columnfamily-and-column)


--------------------------------------------------------------------------------------------------------------------


# HBASE API

## Note

* Under the hood, we are leveraging [hortonworks-spark connector](https://github.com/hortonworks-spark/shc)

### Limitations
* Read/Write API : Doesn't support multi-column ROWKEY
* Read/Write API : Performance is bounded on how HBase table is created

### Design Considerations

Following are the advantages of using SHC
* This provides support for spark hbase integration on Dataframe and Dataset level
* BulkLoad not supported yet
* Writes are converted to HBase Puts and for each partition Puts are executed [code](https://github.com/hortonworks-spark/shc/blob/master/core/src/main/scala/org/apache/spark/sql/execution/datasources/hbase/HBaseRelation.scala)
* [shc-github](https://github.com/hortonworks-spark/shc)

Following are the limitations if one writes own write implementations via JAVA Client for HBASE
* Only updates on single or very few rows should be performed using this
* If we use this option, we have to sequentially run all the puts in driver which takes a lot of time even for small amount of data (>40,000)
* As Put Object is not Serializable, we have to serialize it first and then call it for each partition but all this is done by hortonworks in their Spark HBase Connector (SHC)

___________________________________________________________________________________________________________________



## Create Hive Table pointing to HBase table

The following hive table points to a hbase table named adp_bdpe:test_emp with column families personal and professional

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS `default.hbase_emp`(
  `id` string,
  `name` string,
  `address` string,
  `age` string ,
  `company` string ,
  `designation` string ,
  `salary` string
  )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'hbase.columns.mapping'=':key,personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary',
  'serialization.format'='1')
TBLPROPERTIES (
  'gimel.hbase.table.name'='adp_bdpe:test_emp',
  "gimel.hbase.namespace.name"="adp_bdpe",
  'gimel.storage.type'='HBASE'
  )
```


___________________________________________________________________________________________________________________


## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.hbase.table.name | Y | HBASE Table Name | test | |
| gimel.hbase.namespace.name | Y | HBASE Name Space | default | default |
| hbase.columns.mapping | Y | Key Space in Cassandra | :key,cols:column2,cols:column3 | while writes, this is taken implicitly from DataFrame |
| hbase.rowkey | Y | Mandataory only for write API | id | |
| hbase.columns.specified.flag | N | <br>Used for Write API only<br>true - Write only columns specified in hbase.columns.mapping option<br>false - Write all columns in dataframe<br> | true/false | false |


--------------------------------------------------------------------------------------------------------------------

## Common Imports in all Hbase API Usages

```scala
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.rdd._
import com.paypal.gimel._
import spray.json.DefaultJsonProtocol._;
import spray.json._;
```


___________________________________________________________________________________________________________________


## HBASE Write API Puts

### Prepare Test Data for write

```scala
def stringed(n: Int) = s"""{"id": ${n},"name": "MAC-${n}", "address": "MAC-${n+1}", "age": "${n+1}", "company": "MAC-${n}", "designation": "MAC-${n}", "salary": "${n * 10000}" }"""
val numberOfRows=10000
val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = hiveContext.sparkContext.parallelize(texts)
val dataFrameToWrite: DataFrame = hiveContext.read.json(rdd)
dataFrameToWrite.show
```


### Write all columns

```scala
//Write all columns(also present in ddl) in Dataframe to HBase table

val dataSet: DataSet = DataSet(sparkSession)
val options: Map[String,Any] = Map("gimel.hbase.rowkey"->"id")
val dataFrameWritten = dataSet.write("default.hbase_emp",dataFrameToWrite,options)

```

### Write new columns

```scala
//Write new columns(not in ddl) along with other columns in dataframe to HBase table

val options: Map[String,Any] = Map("gimel.hbase.rowkey"->"id","gimel.hbase.columns.mapping"->"personal:dob")
val dataFrameWritten = dataSet.write("default.hbase_emp",dataFrameToWrite,options)

```

### Write specific columns

```scala
//Write specific columns (given in input option hbase.columns.mapping by user) only to HBase table

val options: Map[String,Any] = Map("gimel.hbase.rowkey"->"id","gimel.hbase.columns.mapping"->"personal:dob", "gimel.hbase.columns.specified.flag"-> true)
val dataFrameWritten = dataSet.write("default.hbase_emp",dataFrameToWrite,options)

```


## HBase Read API Scan

### Read all columns

```scala
//Read all columns from HBase table

val dataFrameRead = dataSet.read("default.hbase_emp")
dataFrameRead.show
```

## Read specific columns

```scala
//Read specific columns(given in input option hbase.columns.mapping by user) from HBase table

val options: Map[String,Any] = Map("gimel.hbase.rowkey"->"id","gimel.hbase.columns.mapping"->"personal:name,professional:salary")
val dataFrameRead = dataSet.read("default.hbase_emp", options)
dataFrameRead.show
```

___________________________________________________________________________________________________________________


## HBase Read API Lookup

### Lookup by rowKey

```scala
//Get all columns of all column families in a row
val options: Map[String,Any] = Map("gimel.hbase.operation"->"get","gimel.hbase.get.filter"->"rowKey=1")
val dataFrameRead = dataSet.read("default.hbase_emp",options)
dataFrameRead.show

```


### Lookup by rowKey and ColumnFamily

```scala
//Get all columns in a column family
val options: Map[String,Any] = Map("gimel.hbase.operation"->"get","gimel.hbase.get.filter"->"rowKey=1:toGet=personal")
val dataFrameRead = dataSet.read("default.hbase_emp",options)
dataFrameRead.show
```


### Lookup by rowKey ColumnFamily and Column

```scala
//Get particular cells
val options: Map[String,Any] = Map("gimel.hbase.operation"->"get","gimel.hbase.get.filter"->"rowKey=1:toGet=personal-name,address|professional-company")
val dataFrameRead = dataSet.read("default.hbase_emp",options)
dataFrameRead.show
```

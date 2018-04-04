* [JDBC Teradata API](#jdbc-teradata-api)
  * [Read API](#read-api)
    * [Create Hive Table](#create-a-teradata-table)
    * [Create Teradata Table](#create-hive-table-pointing-to-teradata-table)
    * [Common Imports](#common-imports)
  * [Write API](#write-api-teradata)
    * [Common Imports](#common-imports)
    * [Create Hive Table](#create-hive-table-pointing-to-teradata-table-1)
    * [Create Teradata Table](#create-teradata-table)
    * [Write API - Batch Mode](#write-into-teradata-via-jdbcdataset-with-batchmode)
    * [Write API - Fast Load](#write-into-teradata-via-jdbcdataset-with-fastload)


--------------------------------------------------------------------------------------------------------------------


# JDBC Teradata API

## Note

* This is an experimental API.
* NOT vetted yet to be used at scale in production.
* Please contact Gimel Team if you consider using this API for your usecases.

--------------------------------------------------------------------------------------------------------------------


## Read API

### Create a Teradata table
```sql
CREATE MULTISET TABLE scratch_db.flights_lookup_cancellation_code ,NO FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO
     (
      code VARCHAR(100) CHARACTER SET LATIN CASESPECIFIC,
      description VARCHAR(4000) CHARACTER SET LATIN CASESPECIFIC)
 PRIMARY INDEX (code) ;

```


### Create Hive Table Pointing to Teradata table

```sql
create external table pcatalog.flights_lookup_carrier_code_td (
      code string,
      description string
      )
LOCATION 'hdfs:///tmp/pcatalog/teradata_smoke_test'
TBLPROPERTIES (
  'gimel.jdbc.driver.class'='com.teradata.jdbc.TeraDriver',
  'gimel.jdbc.input.table.name'='scratch_db.flights_lookup_carrier_code',
  'gimel.jdbc.url'='jdbc:teradata://td_server',
  'gimel.jdbc.output.table.name'='scratch_db.flights_lookup_carrier_code',
  'gimel.storage.type'='JDBC'
);

```


#### Setting password for JDBC datasource
Password for JDBC data sources are specified in the file stored in any HDFS location. If not specified, default the API will look for password file at location ```hdfs:///user/$USER/password/teradata/pass.dat``` . This file will contain password in the format ```jdbc_url/$USER,password``` .

e.g. The password for Teradata can be specified in file  ```hdfs:///user/$USER/password/teradata/pass.dat```  as:

```td_server/USER,PASSWORD```

Password for JDBC data source can be specified as agruments in the APIs in two ways:
1. Spark Configuration parameters:

    Use ```--conf spark.jdbc.p.file=PASSWORD_FILE_PATH```
2. As options in READ/WRITE APIs:

    ```val options= Map(("spark.jdbc.p.file", "hdfs:///user/$USER/password/teradata/pass.dat"))```

#### Options for READ API

##### READ without FASTEXPORT

|    Option     | Default       | Values to specify |
| ------------- | ------------- |-----------------|
| spark.jdbc.username | spark.user | Username |
| spark.jdbc.p.file  | hdfs:///user/$USER/password/teradata/pass.dat  | An HDFS file path containing password |
| charset | ASCII | ASCII,UTF8 |


##### READ with FASTEXPORT

|    Option     | Default       | Values to specify |
| ------------- | ------------- |-----------------|
| spark.jdbc.username | spark.user | Username |
| spark.jdbc.p.file  | hdfs:///user/$USER/password/teradata/pass.dat  | An HDFS file path containing password |
| gimel.jdbc.read.type  | -  | FASTEXPORT |
| charset | ASCII | ASCII,UTF8 |
|SESSIONS|5| Required  number of sessions|
|partitions|4| Required  number of partitions |
|partitionColumn|10| The column on which the data to be partitioned|
|columnLowerBound|0| Lower bound of the values in partition column, if known |
|columnUpperBound|20| Upper bound of the values in partition column, if known |


### Common Imports

```scala
import com.paypal.gimel._
import org.apache.spark._
import org.apache.spark.sql._
```

### Read via JDBCDataSet

```scala
//Initiate DataSet
val dataSet: DataSet = DataSet(sparkSession)
//options "can" be used to specify extra parameters to read data from teradata
 val options = Map(("charset","ASCII"),("spark.jdbc.p.file", "hdfs:///user/$USER/password/teradata/pass.dat"))
//read API
val recsDF = dataSet.read("pcatalog.flights_lookup_carrier_code_td",options)
// Do some usecase
recsDF.show()
```


### Read via JDBCDataSet using FASTEXPORT

```scala
val dataSet: DataSet = DataSet(sparkSession)
//options "can" be used to specify extra parameters to read data from teradata
// To read with FASTEXPORT, set "TYPE" as "FASTEXPORT" and "SESSIONS" to set the number of sessions required.
 val options = Map(("gimel.jdbc.read.type","FASTEXPORT"),("SESSIONS","8"),("charset","ASCII"),("spark.jdbc.p.file", "hdfs:///user/$USER/password/teradata/pass.dat"))
//read API
val recsDF = dataSet.read("pcatalog.flights_lookup_carrier_code_td",options)
// Do some usecase
recsDF.show()
```

--------------------------------------------------------------------------------------------------------------------


## Write API Teradata


#### Options for WRITE API

##### WRITE without FASTLOAD

|    Option     | Default       | Values to specify |
| ------------- | ------------- |-----------------|
| spark.jdbc.username | spark.user | Username |
| spark.jdbc.p.file  | hdfs:///user/$USER/password/teradata/pass.dat  | An HDFS file path containing password |
| charset | ASCII | ASCII,UTF8 |


##### WRITE with FASTLOAD

|    Option     | Default       | Values to specify |
| ------------- | ------------- |-----------------|
| spark.jdbc.username | spark.user | Username |
| spark.jdbc.p.file  | hdfs:///user/$USER/password/teradata/pass.dat  |  An HDFS file path containing password  |
| gimel.jdbc.write.type  | -  | FASTLOAD |
| charset | ASCII | ASCII,UTF8 |
|SESSIONS|5| Required  number of sessions|


### Common Imports in JDBC WRITE API usages below

```scala
import com.paypal.gimel._
import org.apache.spark._
import org.apache.spark.sql._
```


### Write into Teradata via JDBCDataSet with BatchMode

```scala
// Prepare Test Data
def stringed(n: Int) = s"""{"code": "${n}", "description": "MAC-${n}"}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val df: DataFrame = sparkSession.read.json(rdd)
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//DataSet Name
val datasetName = "pcatalog.user_test"
///options "can" be used to specify writing mode, deafult TYPE is batchmode. Set "BATCHSIZE" as required.
val options2 = Map(("charset","ASCII"), ("BATCHSIZE","100000"),("SESSIONS","8"),("spark.jdbc.p.file", "hdfs:///user/$$USER/password/teradata/pass.dat"))
//write some data
dataset.write(datasetName,df,options)
```


### Write into Teradata via JDBCDataSet with FASTLOAD

```scala
// Prepare Test Data
def stringed(n: Int) = s"""{"code": "${n}", "description": "MAC-${n}"}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val df: DataFrame = sparkSession.read.json(rdd)
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//DataSet Name
val datasetName = "pcatalog.user_test"
///options "can" be used to specify writing mode, set "TYPE" as "FASTLOAD" and "SESSIONS" to set the number of sessions required.
val options2 = Map(("teradata.write.type", "FASTLOAD"),("charset","ASCII"),("SESSIONS","8"),("spark.jdbc.p.file", "hdfs:///user/$USER/password/teradata/pass.dat"))
//write some data
dataset.write(datasetName,df,options)
```


### Teradata Update API

Teradata Update API works using write API. All JDBC options for the write are supported in update API as well. Teradata update API adds one more option to update the table: jdbc.insertStrategy
_Requirement_: Teradata table to be updated should have primary keys.


 |    Option     | Default       | Values to specify |
| ------------- | ------------- |-----------------|
| gimel.jdbc.insertStrategy | insert | insert,update,FullLoad,append |

#### Teradata Update table [Update] : ```jdbc.insertStrategy=update```
Gimel Teradata write API updates the table based on primary keys in the table.
 Setting this option, API updates the teradata table where primary key matches the corresponding column in given dataframe.

```scala
// set the update option: jdbc.insertStrategy=update
val options = Map(("spark.jdbc.username","$$USER"),("spark.jdbc.p.file","hdfs:///user/$$USER/password/teradata/pass.dat"),("gimel.jdbc.insertStrategy","update"));
// update Teradata table
val wDF = dset.write("pcatalog.emp_table_write",inputDataFrame,options);
```

#### Teradata Update table [Truncate-Insert] : ```jdbc.insertStrategy=FullLoad```
Gimel Teradata write API updates the table based on primary keys in the table.
Setting this option, API truncates the teradata target table first & then inserts the source dataframe into target table.

```scala
// set the update option: jdbc.insertStrategy=FullLoad
val options = Map(("spark.jdbc.username","$USER"),("spark.jdbc.p.file","hdfs:///user/$USER/password/teradata/pass.dat"),("gimel.jdbc.insertStrategy","FullLoad"));
// update Teradata table
val wDF = dset.write("pcatalog.emp_table_write",inputDataFrame,options);
```

#### Teradata Update table [Update-Insert] : ```jdbc.insertStrategy=append```
Gimel Teradata write API updates the table based on primary keys in the table.
Setting this option, API updates the target teradata table wherever primary key of target table matches corresponding column value in source dataframe. If key is not found in target table, it inserts the corresponding row into target teradata table.

```scala
// set the update option: jdbc.insertStrategy=update
val options = Map(("spark.jdbc.username","$USER"),("spark.jdbc.p.file","hdfs:///user/$USER/password/teradata/pass.dat"),("gimel.jdbc.insertStrategy","update"));
// update Teradata table
val wDF = dset.write("pcatalog.emp_table_write",inputDataFrame,options);
```


-------------------------------------------------------------------------------------------------------------------

## Teradata-To-Any-Storage | JDBC Query PushDown

_ENTIRE TERADATA QUERY WILL BE PUSHED TO TD SERVER, ONLY RESULTS WILL BE RETURNED TO SPARK APP_

### Sample Steps
```scala
// Credentials
sqlContext.setConf("spark.jdbc.username","$USER")
sqlContext.setConf("spark.jdbc.p.file","hdfs:///user/$USER/password/teradata/pass.dat")
// Set this function if your entire select clause is on just one Teradata System
sqlContext.setConf("gimel.jdbc.enableQueryPushdown","true")
// Execute your Query - Entire Query
 com.paypal.gimel.sql.GimeQueryProcessor.executeBatch(
   """
   |insert into pcatalog.gimel_testing_td_to_hive_pxk
   | ANY_LARGE_COMPLEX)SELECT_QUERY_THAT_IS_TERADATA_SQL_COMPLIANT
   |""".strpiMargin ,
   sparkSession);
 // Turn off PushDown and continue with rest of activties
 sqlContext.setConf("gimel.jdbc.enableQueryPushdown","false")
```

--------------------------------------------------------------------------------------------------------------------

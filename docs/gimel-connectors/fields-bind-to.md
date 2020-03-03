

## Fields Bind to Feature

* This feature allows to select additional/missing columns in input dataset while reading data using gimel Data API/GSQL 
* This is mainly used for querying datasets which have dynamic schemas like Kafka/HBase. 
* If a field is present in input dataframe it will take that value, otherwise the default value will be assigned.
* Each field is casted to the data type specified in json or in DataSetProperties of a dataset.
* The fields to bind to can be passed in 2 ways:
  * Json of array of fields with their type and default value
  * Dataset name: It will bind to the fields possessed by that dataset 

--------------------------------------------------------------------------------------------------------------------


## Here is a sample execution

### Fields passed as Json

```bash

// Common Imports
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._
// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")

// c1 column is not present in input data so default value gets assigned
val fieldsBindToString=s"""[{"fieldName":"name","fieldType":"string","defaultValue":"null"},{"fieldName":"address","fieldType":"string","defaultValue":"null"}, {"fieldName":"company","fieldType":"string","defaultValue":""}, {"fieldName":"designation","fieldType":"string","defaultValue":""}, {"fieldName":"c1","fieldType":"int","defaultValue":"4"} ]"""

gsql(s"""set gimel.fields.bind.to.json=$fieldsBindToString""")

val df = gsql("select * from udc.Kafka.Gimel_dev.default.test_topic")

df.show(1)
+--------+----------+-------------+--------------------+---+
|    name|   address|   company|         designation|    c1|
+--------+----------+-------------+--------------------+---+
|    John|  Kery St.|       ABC|             Manager|     4|
+--------+----------+-------------+--------------------+---+

df.printSchema
root
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- company: string (nullable = true)
 |-- designation: string (nullable = true)
 |-- c1: integer (nullable = true)

```

_____________________________________________________________________________________________________________


### Fields taken from dataset

Create test hive dataset for which columns c1 and c2 are not present in input kafka topic

```
CREATE EXTERNAL TABLE `default.test_dataset_fields_bind_to`(
 `name` string,
 `address` string,
 `company` string,
 `designation` string,
 `c1` int
 )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION
'hdfs://cluster/user/bind_to_test';
```

Bind to the fields in above dataset so that even if c1 are not present in input kafka topic, it will not fail while writing data to target hive table

```bash

// Common Imports
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._
// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")

gsql(s"""set gimel.fields.bind.to.dataset=default.test_dataset_fields_bind_to""")

val df = gsql("select * from udc.Kafka.Gimel_dev.default.test_topic")

// c1 is not present in input kafka dataset so default value is assigned
df.show(1)
+--------+----------+-------------+--------------------+---+
|    name|   address|   company|         designation|    c1|
+--------+----------+-------------+--------------------+---+
|    John|  Kery St.|       ABC|             Manager|     4|
+--------+----------+-------------+--------------------+---+


df.printSchema
root
 |-- name: string (nullable = true)
 |-- address: string (nullable = true)
 |-- company: string (nullable = true)
 |-- designation: string (nullable = true)
 |-- c1: integer (nullable = true)

// Write data to above target hive dataset
// Note: The fields should be selected in the order of target dataset as missing fields get added to the end
val sql = s"""insert into default.test_dataset_fields_bind_to 
select `name`,
`address`,
`company`,
`designation`,
`c1`
from udc.Kafka.Gimel_dev.default.test_topic"""

gsql(sql)

```

_____________________________________________________________________________________________________________

### Bind to fields when source is empty and has dynamic schema

##### Example Use case: Handle empty kafka topic consisting of json messages 

Empty dataframe without gimel.fields.bind.to.dataset property

```
spark.sql("set gimel.kafka.throttle.batch.fetchRowsOnFirstRun=0")

gsql(s"""set gimel.fields.bind.to.json=""")

val df = gsql("select * from udc.Kafka.Gimel_dev.default.test_topic")

df.show
++
||
++
++
```

Dataframe with schema with gimel.fields.bind.to.dataset property

```bash

// Common Imports
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._
// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")

// This will fetch no message from kafka
spark.sql("set gimel.kafka.throttle.batch.fetchRowsOnFirstRun=0")

// if this propety is not set, it will return an empty data frame with no schema
gsql(s"""set gimel.fields.bind.to.dataset=default.test_dataset_fields_bind_to""")

val df = gsql("select * from udc.Kafka.Gimel_dev.default.test_topic")

df.show
+--------+----------+-------------+--------------------+---+
|    name|   address|   company|         designation|    c1|
+--------+----------+-------------+--------------------+---+
+--------+----------+-------------+--------------------+---+

```

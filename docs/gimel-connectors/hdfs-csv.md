* [CSV API](#csv-api)
   * [Note](#note)
   * [Create Hive Table](#create-hive-table-csv)
   * [Catalog Properties](#catalog-properties)
   * [Common imports](#common-imports-in-csv-api)
   * [Write CSV](#write-to-csv)
   * [Read CSV](#read-from-csv)

--------------------------------------------------------------------------------------------------------------------

# CSV API

## Note

* This API wraps CSV reader under the hood.
* Read CSV and Write as CSV to HDFS.

___________________________________________________________________________________________________________________


## Create hive table

```
CREATE external TABLE `pcatalog.sample_csv`(
  `payload` string)
LOCATION '/user/LOCATION/csv_data11/csv12'
TBLPROPERTIES (
  'gimel.storage.type'='HDFS',
  'gimel.hdfs.data.format'='csv')
```

___________________________________________________________________________________________________________________


## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.hdfs.save.mode | N | <br>Append - adds data to path<br> Overwrite - Overwrites data in Path<br> | Append | Overwrite |
| gimel.hdfs.csv.data.headerProvided | N | Whether API should infer header from data | <br>true<br>false<br> | true |
| gimel.hdfs.data.format | Y | Property helps API infer whether to invoke CSV reader/writer | csv | |

___________________________________________________________________________________________________________________


## Common Imports
```scala
import com.paypal.gimel.DataSet
import org.apache.spark.sql._

```

___________________________________________________________________________________________________________________


## Write to CSV
```scala
// Prepare Test Data
def stringed(n: Int) = s"""{"id": ${n}, "name": "MAC-${n}", "rev": ${n * 10000}}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val df: DataFrame = sparkSession.read.json(rdd)
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//DataSet Name
val datasetName = "pcatalog.sample_csv"
///Options set by default
///pcatalog.hdfs.save.mode = Overwrite
///User can change the save mode by using Options
val options = Map(("gimel.hdfs.save.mode","Append"))
//write some data
dataset.write(datasetName,df,options)
```

___________________________________________________________________________________________________________________


## Read from CSV
```scala
//Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
//DataSet Name
val datasetName = "pcatalog.sample_csv"
///Options set by default
///pcatalog.csv.data.headerProvided = true
///User can set header to false by using Options
val options = Map(("gimel.hdfs.csv.data.headerProvided","false"))
//Read some data
dataset.read(datasetName,options)
```

___________________________________________________________________________________________________________________

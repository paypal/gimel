* [HDFS API](#csv-api)
   * [Note](#note)
   * [Catalog Properties](#catalog-properties)
   * [Common imports](#common-imports)
   * [Write CSV](#write-to-csv)
   * [Read CSV](#read-from-csv)

--------------------------------------------------------------------------------------------------------------------

# HDFS API

## Note

* This API is meant to read/write from/to HDFS.
* It supports the following formats:
    1. CSV
    2. Sequence
    3. Parquet
    4. Text
    5. ORC
    6. Avro
    7. Json
    8. Gzip
    9. Zip

___________________________________________________________________________________________________________________


## Catalog Properties


| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.hdfs.save.mode | N | <br>Append - adds data to path<br> Overwrite - Overwrites data in Path<br> | Append | Overwrite |
| gimel.hdfs.csv.data.headerProvided | N | Whether API should infer header from data | <br>true<br>false<br> | true |
| gimel.hdfs.data.format | Y | Format of the data stored | text/parquet/csv/orc/sequence/json/gzip/zip/avro | text |
| gimel.hdfs.data.location | Y | HDFS Path of data | hdfs://cluster/user/hadoopuser/test_csv | |
| gimel.fs.row.delimiter | N | Row Delimiter in case of text format | , | \n
| gimel.fs.column.delimiter (gimel.hdfs.column.delimiter) | N | Column delimiter in case of text, sequence and csv formats | , | \u0001 |
| gimel.hdfs.csv.data.inferSchema | N | Infer schema for CSV | true/false | true |
| gimel.hdfs.csv.data.headerProvided | N | Header provided? | true/false | true |
| gimel.hdfs.data.compressionCodec | N | Compression Codec for text files | gzip/snappy/uncompressed | snappy |
| gimel.hdfs.data.crosscluster.threshold | N | Threshold for Cross cluster in GB | 50 | 50 |

___________________________________________________________________________________________________________________


## Common Imports
```scala
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}

```
___________________________________________________________________________________________________________________


## Write to CSV file
```scala
// Prepare Test Data
def stringed(n: Int) = s"""{"id": ${n}, "name": "MAC-${n}", "rev": ${n * 10000}}"""
val texts: Seq[String] = (1 to 100).map { x => stringed(x) }.toSeq
val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
val df: DataFrame = sparkSession.read.json(rdd)

// Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
// DataSet Name
val datasetName = "Hdfs.Cluster.default.cluster_user_hadoopuser_csv_test"

// Options set by default
// gimel.hdfs.save.mode = Overwrite
// User can change the save mode by using Options
val options = Map(("gimel.hdfs.save.mode", "Append"))
// Write some data
dataset.write(datasetName, df, options)
```

___________________________________________________________________________________________________________________


## Read CSV

```scala
// Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
// DataSet Name
val datasetName = "Hdfs.Cluster.default.cluster_user_hadoopuser_csv_test"

// User can set header to false by using Options
val options = Map(("gimel.hdfs.csv.data.headerProvided", "false"))
// Read some data
dataset.read(datasetName, options)
```

___________________________________________________________________________________________________________________

## Read Sequence file by passing the properties at runtime

```scala
// Initiate DataSet
val dataset = com.paypal.gimel.DataSet(sparkSession)
// DataSet Name
val datasetName = "Hdfs.Cluster.default.cluster_user_hadoopuser_seq_test"

// Options passed at runtime to overwrite the ones coming from UDC
val options = Map("rest.service.method" -> "https",
"rest.service.host" -> "udc-service-host",
"rest.service.port" -> "443",
"gimel.hdfs.data.format" -> "sequence",
"gimel.hdfs.nn" -> "hdfs://cluster-hdfs.example.com:8020/",
"gimel.hdfs.data.location" -> "hdfs://cluster/user/hadoopuser/seq_test")

val df = dataset.read(datasetName, options)
```
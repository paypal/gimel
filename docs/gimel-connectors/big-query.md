
* [Big Query API](#big-query-api)
* [Create Hive Catalog Table](#create-hive-catalog-table)
* [Catalog Properties](#catalog-properties)
* [Big Query API Usage](#big-query-api-usage)


--------------------------------------------------------------------------------------------------------------------


# Big Query API


## Create Hive Catalog Table


```sql
drop table if exists default.my_dataset1;

create external table default.my_dataset1
(
cols String
)
location "gs://demc-test/dev-dataproc-gcs.BQ_benchmark.date_dim1"
TBLPROPERTIES(
'gimel.storage.type'='bigquery',
'temporaryGcsBucket'='demc-test',
'gimel.bigquery.table'='dev-dataproc-gcs.BQ_benchmark.date_dim1'
)
```

--------------------------------------------------------------------------------------------------------------------

## Catalog Properties

Refer [Big Query Docs](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) for indepth details.

| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| gimel.bigquery.table | Y | Big Query Table | project.dataset.table | |

--------------------------------------------------------------------------------------------------------------------


## Big Query API Usage

### Common Imports in all API Usages

```scala
val dataset = com.paypal.gimel.DataSet(spark)
```

-------------------------------------------------------------------------------------------------------------------

### Read API Usage

```scala
// Set Catalog Provider to Hive
spark.sql("set gimel.catalog.provider=HIVE")

// Read as DataFrame
val df1 = dataset.read("default.my_dataset1")

df1.show()
```

--------------------------------------------------------------------------------------------------------------------

### Write API Usage

```scala
// Write Dataframe to another Dataset ( In this case - big query )
dataset.write("default.my_dataset1",df1)

// Working with options
val options = Map("saveMode" -> "ErrorIfExists")
// Write must Error out ( as expected )
dataset.write("default.my_dataset1",df1,options)
```

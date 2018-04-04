* [Catalog Providers](#catalog-providers)
  * [Note](#note)
  * [Catalog Provider - USER](#catalog-provider-as-user)
    * [GSQL](#gsql)
    * [Data API Usage](#data-api-usage)
  * [Catalog Provider - HIVE](#catalog-provider-as-hive)
  * [Catalog Provider - PCATALOG](#catalog-provider-as-pcatalog)
  
  
 --------------------------------------------------------------------------------------------------------------------


# Catalog Providers

## Note

* Provides different ways to specify DataSet Properties to Read/Write Data API
* Can be set using gimel.catalog.provider property
* Can be set to USER/HIVE/PCATALOG

## Catalog Provider as USER

### Note

* Uses DataSetProperties specified by user as Json String or DataSetProperties Object
* Recommended to be used only for testing/exploring purposes

### GSQL
* Set the catalog provider property to USER

```
%%sql 
set gimel.catalog.provider=USER
```

* Set DataSetProperties Json

Note: The property name should be {datasetName}.dataSetProperties. 
For Example if your dataset name is pcatalog.mydataset, then the property name would be pcatalog.mydataset.dataSetProperties.

Example for HBase Dataset

```
%%sql
set pcatalog.hbase_test.dataSetProperties={
    "datasetType": "HBASE",
    "fields": [
        {
            "fieldName": "id",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "name",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "address",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "age",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "company",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "designation",
            "fieldType": "string",
            "isFieldNullable": false
        },
        {
            "fieldName": "salary",
            "fieldType": "string",
            "isFieldNullable": true
        }
    ],
    "partitionFields": [],
    "props": {
        "gimel.hbase.rowkey":"id",
        "gimel.hbase.table.name":"adp_bdpe:test_annai",
        "gimel.hbase.namespace.name":"namespace",
        "gimel.hbase.columns.mapping":"personal:name,personal:address,personal:age,professional:company,professional:designation,professional:salary"
    }
}
```


* Run the sql

```
%%pcatalog
select * from pcatalog.hbase_test
```

### DATA API Usage

* Imports and Initializations

```
import org.apache.spark.sql.{DataFrame, SQLContext};
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.{SparkConf, SparkContext};
import org.apache.spark.rdd.RDD;
import com.paypal.gimel.logger.Logger;
import com.paypal.gimel.{DataSet, DataSetType};
import spray.json.DefaultJsonProtocol._;
import spray.json._;
val logger = Logger(this.getClass.getName)
import org.apache.spark.sql.{DataFrame, SaveMode, SQLContext}
import com.paypal.gimel.common.catalog._

//Initiate HiveContext
val hiveContext = new HiveContext(sc);
val dataSet = com.paypal.gimel.DataSet(spark);
val sqlContext = hiveContext.asInstanceOf[SQLContext]

```

* Usage

```
hiveContext.sql("set gimel.catalog.provider=USER");

val datasetPropsJson="""
{
    "datasetType": "ELASTIC_SEARCH",
    "fields": [],
    "partitionFields": [],
    "props": {
        "gimel.es.index.partition.isEnabled": "true",
        "gimel.es.index.partition.suffix": "20180120",
        "datasetName": "data_set_name",
        "nameSpace": "pcatalog",
        "es.port": "8080",
        "es.resource": "index/type",
        "gimel.es.index.partition.delimiter": "-",
        "es.nodes": "http://es_host"
    }
}
"""

val data = dataSet.read("pcatalog.data_set_name",
                         Map("pcatalog.data_set_name.dataSetProperties" -> datasetPropsJson))
```

## Catalog Provider as HIVE

### Note

* Uses Dataset Properties from the external hive table
* Set the catalog provider property to HIVE

```
%%sql 
set gimel.catalog.provider=HIVE
```

### Create Hive Table pointing to physical storage

You can find hive table templates for each storage in their docs [Stack & Version Compatibility](../README.md#stack-&-version-compatibility)

## Catalog Provider as PCATALOG

### Note

* Uses DataSetProperties from the PCatalog
* In order to use this, storage system of the Dataset should be onborded in Pcatalog
* Set the catalog provider property to PCATALOG

```
%%sql 
set gimel.catalog.provider=PCATALOG
```




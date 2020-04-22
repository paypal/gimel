* [Catalog Providers](#catalog-providers)
  * [Note](#note)
  * [Catalog Provider - USER](#catalog-provider-as-user)
    * [GSQL](#gsql)
    * [Data API Usage](#data-api-usage)
  * [Catalog Provider - HIVE](#catalog-provider-as-hive)
  * [Catalog Provider - UDC](#catalog-provider-as-udc-unified-data-catalog)
  
  
 --------------------------------------------------------------------------------------------------------------------


# Catalog Providers

## Note

* Provides different ways to specify DataSet Properties to Read/Write Data API
* Can be set using gimel.catalog.provider property
* Can be set to USER/HIVE/UDC

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

## Catalog Provider as UDC (Unified Data Catalog)

### Note

* Uses DataSetProperties from the UDC
* In order to use this, storage system of the Dataset should be onboarded in UDC
* Set the catalog provider property to UDC

```
%%sql 
set gimel.catalog.provider=UDC
```

### UDC parameters

| Property | Mandatory? | Description | Example | Default |
|----------|------------|-------------|------------|-------------------|
| rest.service.method or spark.rest.service.method | Y | UDC Rest service protocol  | http/https | https |
| rest.service.host	or spark.rest.service.host| Y | UDC Rest service host name  | my-udc.example.com | None|
| rest.service.port or spark.rest.service.port | Y | UDC Rest service port  | 80 | 443 |
| gimel.udc.auth.enabled or spark.gimel.udc.auth.enabled | N | UDC auth enabled flag  | true/false | true |
| gimel.udc.auth.provider.class or spark.gimel.udc.auth.provider.class | N | This is a Must if `gimel.udc.auth.enabled=true`  | com.example.MyAuthProvider | None |

### UDC Auth

* In order to connect to UDC for getting DatasetProperties, UDC-APP-NAME and UDC-AUTH-KEY has to be set in header for GET calls.
* Add a custom AuthProvider in order to retrieve the Auth token (UDC-AUTH-KEY value) from any service or key store.
* Add gimel-common dependency in your project
 
 ```xml
  <dependency>
    <groupId>com.paypal.gimel</groupId>
    <artifactId>gimel-common</artifactId>
    <version>${gimel.version}</version>
   </dependency>
 ```

* Extend com.paypal.gimel.common.security.AuthProvider in gimel-common and implement getCredentials method which should return the auth token.
Example:

```scala
package com.example

import com.paypal.gimel.common.security._

class MyAuthProvider extends AuthProvider {

  override def getCredentials(props: Map[String, Any]): String = {
    /*
     * Implement your own logic here
     */
  }
}
```

Here props are the gimel properties passed at runtime. 

```scala
import org.apache.spark.sql.{Column, Row, SparkSession,DataFrame}
import org.apache.spark.sql.functions._

// Create Gimel SQL reference
val gsql: (String) => DataFrame = com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_: String, spark)
gsql("set gimel.logging.level=CONSOLE")

val options = Map("rest.service.method" -> "https",
"rest.service.host" -> "my-udc.example.com",
"rest.service.port" -> "443",
"gimel.deserializer.class" -> "com.paypal.gimel.deserializers.generic.JsonDynamicDeserializer",
"gimel.kafka.throttle.batch.fetchRowsOnFirstRun" -> "1",
"gimel.udc.auth.provider.class" -> "com.example.MyAuthProvider")

val df = dataset.read("udc.Kafka.Gimel_Dev.default.flights", options)
df.show

```



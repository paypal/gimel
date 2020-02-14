
* [SFTP API](#SFTP-api)
  * [Overview](#Overview)
  * [Design Considerations](#design-considerations)
  * [Create Hive Table Catalog](#Create-Hive-Table-Catalog)
  * [Supported File Types](#supported-file-types)
  * [Password Options](#Password-Options)
  * [SFTP GIMEL Read API for CSV](#SFTP-GIMEL-Read-API-for-CSV)
  * [SFTP GIMEL Write API JSON](#SFTP-GIMEL-Write-API-JSON)
  * [SFTP GIMEL GSQL](#SFTP-GIMEL-GSQL)
  * [SFTP_GIMEL_READ_WRITE_FOR_CML](#SFTP-GIMEL-READ-WRITE-FOR-XML)
  * [Limitations](#Limitations)
  
  



--------------------------------------------------------------------------------------------------------------------


# SFTP API
* This API will enable read, write files into SFTP servers


--------------------------------------------------------------------------------------------------------------------


## Design Considerations

### Spark SFTP connector 

* https://github.com/springml/spark-sftp
* SFTP spark is a library for constructing dataframes by downloading files from SFTP and writing dataframe to a SFTP server
* Gimel connector is using this library as dependency


--------------------------------------------------------------------------------------------------------------------

## Create Hive Table Catalog

The following hive table points to SFTP server

```sql
  CREATE EXTERNAL TABLE pcatalog.sftp_drop_zone (
        payload string)
      ROW FORMAT SERDE
        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
      STORED AS INPUTFORMAT
        'org.apache.hadoop.mapred.TextInputFormat'
      OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      LOCATION
        'hdfs://cluster1/tmp/pcatalog/sftp_drop_zone'
      TBLPROPERTIES (
        'gimel.sftp.host'='sftp_server',
        'gimel.storage.type'='SFTP')
```

--------------------------------------------------------------------------------------------------------------------

## Supported File Types

* CSV
* JSON
* AVRO
* PARQUET
* TXT
* XML

--------------------------------------------------------------------------------------------------------------------

## Password Options

* The passwords can be given using either local file sytem or HDFS file system
* for local file system, we need to put the password in a file and mention the file as below options
* This will be useful in default or yarn client mode

    "gimel.sftp.file.password.strategy" -> "file"
    "gimel.sftp.file.password.source" -> "local"
    "gimel.sftp.file.password.path" -> "/x/home/xxxx/mypass.txt"
              
* for hdfs file system, we need to put the password in a file and mention the file as below options.
* This will be useful in yarn cluster mode

    "gimel.sftp.file.password.strategy" -> "file"
    "gimel.sftp.file.password.source" -> "hdfs"
    "gimel.sftp.file.password.path" -> "hdfs://cluster/xxxx/mypass.txt"
              

--------------------------------------------------------------------------------------------------------------------

## SFTP GIMEL Read API for CSV 

* The following example says how to give the password for SFTP server using local file system as source

```scala
val options = Map("gimel.sftp.username" -> "USERNAME",
                  "gimel.sftp.file.password.strategy" -> "file"
                  "gimel.sftp.file.password.source" -> "local",
                  "gimel.sftp.file.password.path" -> "/x/home/xxxx/mypass.txt",
                  "hdfsTempLocation" -> "/tmp/basu", 
                  "header" -> "false", 
                  "gimel.sftp.filetype" -> "csv", 
                  "gimel.sftp.file.location" -> "bus_use.csv" )
val sftpDF = dataSet.read("pcatalog.SFTP.SFTP_SERVER.default.Files", options )
sftpDF.show

```


--------------------------------------------------------------------------------------------------------------------


## SFTP GIMEL Write API JSON

* The following example says how to give the password for SFTP server using HDFS file as source

```scala
val options = Map("gimel.sftp.username" -> "USERNAME",
                  "gimel.sftp.file.password.strategy" -> "file"
                  "gimel.sftp.file.password.source" -> "hdfs",
                  "gimel.sftp.file.password.path" -> "hdfs://cluster1/user/xxxxxx/mypass.txt",
                  "hdfsTempLocation" -> "/tmp/basu", 
                  "header" -> "false", 
                  "gimel.sftp.filetype" -> "json", 
                  "gimel.sftp.file.location" -> "myJsonNew.json" )
val sftpDFRes = dataSet.write("pcatalog.SFTP.SFTP_SERVER.default.Files", sftpDF, options )
```


## SFTP GIMEL GSQL

``` scala
spark.sql("set gimel.sftp.username=USERNAME")
spark.sql("set gimel.sftp.password=*****")
spark.sql("set gimel.sftp.filetype=csv")
spark.sql("set gimel.sftp.file.location=bus_use.csv")
spark.sql("set header=false")
spark.sql("set gimel.jdbc.password.strategy=file")
spark.sql("set hdfsTempLocation=/tmp/basu")
spark.sql("set gimel.jdbc.p.file=hdfs://cluster1/user/USERNAME/pass.dat")

val newDF = com.paypal.gimel.scaas.GimelQueryProcessor.executeBatch("create table pcatalog.teradata.mycluster.test_db.myTable1 as select * from pcatalog.SFTP.SFTP_SERVER.default.Files",spark)

val sampleDF = com.paypal.gimel.scaas.GimelQueryProcessor.executeBatch("insert into pcatalog.teradata.mycluster.test_db.myTable1 as select * from pcatalog.SFTP.SFTP_SERVER.default.Files",spark)

// Here pcatalog.SFTP.SFTP_SERVER.default.Files is pcatalog table created through gimel pcatalog UI
// This is created by pointing sftp_server of paypal corp.


```

## SFTP GIMEL READ WRITE FOR XML

```
// Read xml 
val options = Map("gimel.sftp.file.password.strategy" -> "file",
                        "gimel.sftp.file.password.source" -> "local",
                        "gimel.sftp.file.password.path" -> "/x/home/USER/mypass.txt",
                        "gimel.sftp.username" -> "username",
                        "header" -> "false",
                        "hdfsTempLocation" -> "hdfs://cluster1/user/username/",
                        "gimel.sftp.filetype" -> "xml",
                       "rowTag" -> "YEAR",
                         "gimel.sftp.file.location" -> "myxml.xml" );
val sftpDFRes = dataSet.read("pcatalog.SFTP.SFTP_SERVER.default.Files", options )

// Write XML

val options = Map("gimel.sftp.file.password.strategy" -> "file",
                        "gimel.sftp.file.password.source" -> "local",
                        "gimel.sftp.file.password.path" -> "/x/home/USER/mypass.txt",
                        "gimel.sftp.username" -> "username",
                        "header" -> "false",
                        "hdfsTempLocation" -> "hdfs://cluster1/user/username/",
                        "gimel.sftp.filetype" -> "xml",
                       "rowTag" -> "YEAR",
                       "rootTag" -> "YEARS",
                         "gimel.sftp.file.location" -> "myxml.xml" );
dataSet.write("pcatalog.SFTP.SFTP_SERVER.default.Files", sftpDFRes, options )



```

## Limitations

* While writing files to SFTP we have to specify the file names ending with .gz as it is stored in zipped format.
* Else the write API adds .gz as the suffix and store the file in SFTP server
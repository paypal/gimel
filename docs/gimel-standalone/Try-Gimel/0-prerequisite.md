
* [Gimel Standalone](#gimel-standalone)
   * [Overview](#overview)
   * [Install Docker](#install-docker)
   * [Download the Gimel Jar](#download-the-gimel-jar)
   * [Run Gimel Quickstart Script](#run-gimel-quickstart-script)
   * [Common Imports and Initializations](#common-imports-and-initializations)
   
# Gimel Standalone

## Overview

* The Gimel Standalone feature will provide capability for developers / users alike to

  * Try Gimel in local/laptop without requiring all the ecosystems on a hadoop cluster.
  * Standalone would comprise of docker containers spawned for each storage type that the user would like to explore. Storage type examples : kafka , elasticsearch.
  * Standalone would bootstrap these containers (storage types) with sample flights data.
  * Once containers are spawned & data is bootstrapped, the use can then refer the connector docs & try the Gimel Data API / Gimel SQL on the local laptop.
  * Also in the future : the standalone feature would be useful to automate regression tests & run standalone spark JVMs for container based solutions. 

___________________________________________________________________________________________________________________

## Install Docker

* Install docker on your machine 
  * MAC - https://docs.docker.com/docker-for-mac/install/

___________________________________________________________________________________________________________________

## Download the Gimel Jar

* Download the gimel jar from [Here](https://drive.google.com/uc?id=1mVia6-dTyX9ZU2-r91TFJu4_hEhapVRA&export=download)
* Move it to gimel/gimel-dataapi/gimel-standalone/lib folder

___________________________________________________________________________________________________________________

## Run Gimel Quickstart Script

```
$ quickstart/gimel {STORAGE_SYSTEM}
```

* STORAGE_SYSTEM can be either ```all``` or comma seperated list like as follows
```
$ quickstart/gimel kafka,elasticsearch,hbase
```
* This script will do the following:
  * Start docker containers for each storage
  * Bootstrap the physical storages (Create Kafka Topic and HBase tables)
  * Start a spark-shell with gimel jar added
  
Note: If you want to start your own spark-shell, Run the following command

```
docker exec -it spark-master bash -c \
"export USER=an;export SPARK_HOME=/spark/;export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin; \
/spark/bin/spark-shell --jars /root/gimel-sql-1.2.0-SNAPSHOT-uber.jar"
```
  
___________________________________________________________________________________________________________________

## Common Imports and Initializations

```
import org.apache.spark.sql.{DataFrame, SQLContext};
import org.apache.spark.sql.hive.HiveContext;
import com.paypal.gimel.sql.GimelQueryProcessor

val gsql = GimelQueryProcessor.executeBatch(_:String,spark)
```

___________________________________________________________________________________________________________________


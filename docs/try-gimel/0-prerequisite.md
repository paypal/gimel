
* [Summary](#summary)
* [Gimel Standalone](#gimel-standalone)
   * [Overview](#overview)
   * [Install Docker](#install-docker)
   * [Download the Gimel Jar](#download-the-gimel-jar)
   * [Run Gimel Quickstart Script](#run-gimel-quickstart-script)
   * [Common Imports and Initializations](#common-imports-and-initializations)
   
# Summary

 * Install Docker
 * **Note:** *At any point if there is any failure , power down gimel  ```quickstart/stop-gimel down```*
 * Clone Gimel Repo 
 * Download Gimel Jar
 * Run the bootstrap module
 * Once spark-session is ready : play with Gimel Data API / GSQL
 
 
 
___________________________________________________________________________________________________________________

# Gimel Standalone

## Overview
The Gimel Standalone feature will provide capability for developers / users alike to

  * Try Gimel in local/laptop without requiring all the ecosystems on a hadoop cluster.
  * Standalone would comprise of docker containers spawned for each storage type that the user would like to explore. Storage type examples : kafka , elasticsearch.
  * Standalone would bootstrap these containers (storage types) with sample flights data.
  * Once containers are spawned & data is bootstrapped, the use can then refer the connector docs & try the Gimel Data API / Gimel SQL on the local laptop.
  * Also in the future : the standalone feature would be useful to automate regression tests & run standalone spark JVMs for container based solutions. 

___________________________________________________________________________________________________________________

## Install Docker

* Install docker on your machine 
  * MAC - <a href="https://docs.docker.com/docker-for-mac/install/" target="_blank">Docker Installation</a>
  * Start Docker Service
  * Increase the memory by navigating to Preferences > Advanced > Memory
  * (Optional) Clear existing containers and images
      * Check for existing Docker containers running - ```docker ps -aq```
      * Kill existing Docker containers (if any) - ```docker kill $(docker ps -aq)```
      * Remove existing Docker containers(if any) - ```docker rm $(docker ps -aq)```
  

___________________________________________________________________________________________________________________

## Download the Gimel Jar

* Clone the repo <a href="https://github.com/paypal/gimel" target="_blank">Gimel</a>
* Download the gimel jar from <a href="https://drive.google.com/uc?id=1mVia6-dTyX9ZU2-r91TFJu4_hEhapVRA&export=download" target="_blank">Here</a>
* Navigate to the folder gimel

```
cd gimel
```
* Navigate to the folder gimel-dataapi/gimel-standalone/ - ```cd gimel-dataapi/gimel-standalone/```
* Create lib folder in gimel-standalone - ```mkdir lib```
* Copy the downloaded jar in lib

___________________________________________________________________________________________________________________

## Run Gimel Quickstart Script

* Navigate back to GIMEL_HOME
```
cd $GIMEL_HOME
```

* To install all the dockers and bootstrap storages, please execute the following command
```
quickstart/start-gimel {STORAGE_SYSTEM}
```

* STORAGE_SYSTEM can be either ```all``` or comma seperated list like as follows
```
quickstart/start-gimel kafka,elasticsearch,hbase-master,hbase-regionserver
```

**Note:** *This script will do the following*
  * *Start docker containers for each storage*
  * *Bootstrap the physical storages (Create Kafka Topic and HBase tables)*
  
  
* To start the spark shell run the following command

```
docker exec -it spark-master bash -c \
"export USER=an;export SPARK_HOME=/spark/;export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin; \
/spark/bin/spark-shell --jars /root/gimel-sql-2.0.0-SNAPSHOT-uber.jar"
```

**Note:** *You can view the Spark UI  <a href="http://localhost:4040" target="_blank">here</a>*
___________________________________________________________________________________________________________________

## Common Imports and Initializations

```
import org.apache.spark.sql.{DataFrame, SQLContext};
import org.apache.spark.sql.hive.HiveContext;
import com.paypal.gimel.sql.GimelQueryProcessor

val gsql = GimelQueryProcessor.executeBatch(_:String,spark)
```

___________________________________________________________________________________________________________________


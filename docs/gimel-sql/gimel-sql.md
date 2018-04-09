
* [SQL API](#sql-api)
  * [SQL Output Control](#controlling-sql-output-and-behaviour)
  * [SQL On spark-shell | spark-submit](#sql-on-scala-spark-shell--spark-submit)
    * [Batch Mode SQL](#execute-batch-sql-1)
    * [Streaming SQL](#execute-stream-sql-1)


--------------------------------------------------------------------------------------------------------------------

# SQL API

* _Simply Combine all your DataSets & express your entire Logic as a single SQL via SQL API._
* _In a Single SQL - Join data from various systems (Kafka, ES, HBASE, Hive, Teradata, more to come)._
* _Execute your SQL in Batch Mode or Continuous Streaming Mode._


## Controlling SQL output and behaviour

| Property for SQL results | Notes |
|--------------------------|-------|
|gimel.query.results.show.rows.only | Set this to "true" to stop getting all these messages. (Default : false) |
|gimel.query.results.show.rows.threshold | Number of rows to display in interactive mode (Default : 1000) |

------------------------------------------------------------------------------------------------------

## SQL on Scala (spark-shell | spark-submit)

### Execute Batch SQL - Spark Shell / Program

```scala
sparkSession.conf.set("gimel.query.results.show.rows.only","true")

sparkSession.sql("set gimel.kafka.throttle.streaming.window.seconds=20");
sparkSession.sql("set gimel.kafka.throttle.streaming.parallelism.factor=20");
sparkSession.sql("set gimel.kafka.kafka.reader.checkpoint.save=true");
sparkSession.sql("set gimel.kafka.kafka.reader.checkpoint.clear=false");
sparkSession.sql("set gimel.kafka.throttle.batch.fetchRowsOnFirstRun=100");
sparkSession.sql("set gimel.kafka.throttle.batch.maxRecordsPerPartition=50");
sparkSession.sql("set gimel.logging.level=INFO");
sparkSession.sql("set gimel.query.results.show.rows.only=true");

val gsql=com.paypal.gimel.sql.GimelQueryProcessor.executeBatch(_:String,sparkSession);
val df = gsql("SELECT count(*) FROM pcatalog.flights_kafka");
```

### Execute Stream SQL - Spark Shell / Program

```scala
val gsqlStream=com.paypal.gimel.sql.GimelQueryProcessor.executeStream(_:String,sparkSession);
gsqlStream("INSERT into pcatalog.flights_elastic SELECT count(*) FROM pcatalog.flights_kafka");
```



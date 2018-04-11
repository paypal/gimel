/*
 * Copyright 2018 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.paypal.gimel.sql

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.Time

import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf._
import com.paypal.gimel.common.utilities.DataSetUtils.resolveDataSetName
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities.JDBCAuthUtilities
import com.paypal.gimel.kafka.conf.KafkaConfigs
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.logging.GimelStreamingListener


object GimelQueryUtils extends Logger {

  /**
    * Returns the individual works from the SQL as tokens
    *
    * @param sql SqlString
    * @return String tokens
    */
  def tokenizeSql(sql: String): Array[String] = sql.replaceAllLiterally("\n", " ").replaceAllLiterally("\t", " ").split(" ")

  /**
    * This search will return true if the hive query has a partitioning criteria in it.
    *
    * @param sql SqlString
    * @return true, if query contains ; insert into partitions of target table.
    */
  def isQueryContainingPartitioning(sql: String): Boolean = {
    val tokens = tokenizeSql(sql.toLowerCase)
    var isHaving = false
    var tmp = ""
    tokens.foreach { token =>
      if ((tmp == "partition" & token == "(") || tmp.contains("partition(")) isHaving = true
      tmp = token
    }
    isHaving
  }

  /**
    * Gets the Tables List from SQL
    *
    * @param sql SQL String
    * @return List of Tables
    */
  def getTablesFrom(sql: String): Array[String] = {
    val sqlLower = sql.toLowerCase
    val searchList = List("insert", "select", "from", "join", "where")
    var lastKey = if (searchList.contains(tokenizeSql(sqlLower).head)) {
      tokenizeSql(sqlLower).head
    } else {
      ""
    }
    var currentKey = ""
    var pCatalogTables = List[String]()
    // Pick each pcatalog.table only if its appearing at specific places in the SQL String
    // This guard necessary if someone uses "pcatalog" as an alias
    tokenizeSql(sqlLower).tail.foreach {
      token =>
        currentKey = if (searchList.contains(token)) token else currentKey
        val pickCriteriaMet = token.contains("pcatalog") && token.contains(".")
        if (pickCriteriaMet) {
          if (lastKey == "from" & !(currentKey == "select")) pCatalogTables ++= List(token)
          if (lastKey == "join" & !(currentKey == "select")) pCatalogTables ++= List(token)
        }
        lastKey = if (searchList.contains(token)) currentKey else lastKey
        currentKey = ""
    }
    pCatalogTables.toArray
  }


  /**
    * Prints Stats for Streaming Batch Window
    *
    * @param time     Time Object - Spark Streaming
    * @param listener GIMEL Streaming Listener
    */
  def printStats(time: Time, listener: GimelStreamingListener): Unit = {
    val batchTimeMS = time.milliseconds.toString
    val batchDate = new Date(batchTimeMS.toLong)
    val df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val batchTime = df.format(batchDate)
    info(s"Current Batch ID --> $time | $batchTime | $batchDate")
    info(
      s"""|-----------------------------------------------------------------------
          |Batch ID -->
          |-----------------------------------------------------------------------
          |time               : $time
          |batchTimeMS        : $batchTimeMS
          |batchTime          : $batchTime
          |-----------------------------------------------------------------------
          |Listener Metrics -->
          |-----------------------------------------------------------------------
          |appProcessingDelay : ${listener.appProcessingDelay}
          |appSchedulingDelay : ${listener.appSchedulingDelay}
          |appTotalDelay      : ${listener.appTotalDelay}
          |processingDelay    : ${listener.processingDelay}
          |schedulingDelay    : ${listener.schedulingDelay}
          |totalDelay         : ${listener.totalDelay}
          |-----------------------------------------------------------------------
          |""".stripMargin)
  }

  /**
    * Cache the DataSet (lazily) if its configured to be cached - by user in properties.
    *
    * @param df          DataFrame
    * @param dataSetName DataSetName representing the DataFrame
    * @param options     Props
    */
  def cacheIfRequested(df: DataFrame, dataSetName: String, options: Map[String, String]): Unit = {
    val isCachingEnabled = (
      options.getOrElse(GimelConstants.DATA_CACHE_IS_ENABLED, "false").toBoolean
        && (
        options.getOrElse(s"${GimelConstants.DATA_CACHE_IS_ENABLED}.for.$dataSetName", "false").toBoolean
          || options.getOrElse(s"${GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL}", "false").toBoolean
        )
      )
    if (isCachingEnabled) df.cache()
  }

  /**
    * Push downs the SELECT query to JDBC data source and executes using JDBC read.
    *
    * @param selectSQL    SELECT SQL string
    * @param sparkSession : SparkSession
    * @return DataFrame
    */

  def executePushdownQuery(selectSQL: String, sparkSession: SparkSession): DataFrame = withMethdNameLogging { methodName =>
    val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)
    val usernameConf: Map[String, String] = sparkSession.conf.getAll.contains(JdbcConstants.jdbcUserName) match {
      case true => Map(JdbcConstants.jdbcUserName -> sparkSession.conf.get(JdbcConstants.jdbcUserName))
      case _ => Map()
    }
    val pConf: Map[String, String] = sparkSession.conf.getAll.contains(JdbcConfigs.jdbcP) match {
      case true => Map(JdbcConfigs.jdbcP -> sparkSession.conf.get(JdbcConfigs.jdbcP))
      case _ => Map()
    }
    val dataSetProps = usernameConf ++ pConf
    info(s"datasetProps:${dataSetProps}")
    val (username, password) = authUtilities.getJDBCCredentials(sparkSession.conf.get(JdbcConfigs.jdbcUrl), dataSetProps)
    val pushdownSQL = "( " + selectSQL + " )" + " AS tempPushDownTable"
    val jdbcOptions: Map[String, String] = Map("url" -> sparkSession.conf.get(JdbcConfigs.jdbcUrl),
      "driver" -> sparkSession.conf.get(JdbcConfigs.jdbcDriverClassKey),
      "dbtable" -> pushdownSQL, "user" -> username, "password" -> password)

    info(s"Final SQL for Query Push Down --> ${pushdownSQL}")

    sparkSession
      .read
      .format("jdbc")
      .options(jdbcOptions)
      .load()

  }

  /**
    * Resolves the Query by replacing Tmp Tables in the Query String
    * For Each Tmp Table placed in the Query String - a DataSet.read is initiated
    * For each Tmp Table - if the dataset is a Kafka DataSet - then each KafkaDataSet object is accumulated
    * Accumulated KafkaDataSet Object will be used towards the end of the Query (on success) -
    * to call check pointing for each topic consumed
    *
    * @param originalSQL  SQLString
    * @param selectSQL    SQLString
    * @param sparkSession : SparkSession
    * @param dataSet      Dataset Object
    * @return Tuple of (Resolved Original SQL, Resolved Select SQL, List of (KafkaDataSet)
    */
  def resolveSQLWithTmpTables(originalSQL: String, selectSQL: String, sparkSession: SparkSession, dataSet: com.paypal.gimel.DataSet): (String, String, List[com.paypal.gimel.kafka.DataSet]) = withMethdNameLogging { methodName =>
    var kafkaDataSets: List[com.paypal.gimel.kafka.DataSet] = List()
    var sqlTmpString = selectSQL
    var sqlOriginalString = originalSQL
    val pCatalogTablesToReplaceAsTmpTable: Map[String, String] = getTablesFrom(selectSQL).map {
      eachSource =>
        val options = getOptions(sparkSession)._2
        val df = dataSet.read(eachSource, options)
        cacheIfRequested(df, eachSource, options)
        if (dataSet.latestKafkaDataSetReader.isDefined) {
          info(s"@${methodName} | Added Kafka Reader for Source --> $eachSource")
          kafkaDataSets = kafkaDataSets ++ List(dataSet.latestKafkaDataSetReader.get)
        }
        val tabNames = eachSource.split("\\.")
        val tmpTableName = "tmp_" + tabNames(1)
        df.createOrReplaceTempView(tmpTableName)
        (eachSource, tmpTableName)
    }.toMap
    val queryPushDownFlag: String = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false")
    queryPushDownFlag match {
      case "true" =>
        info("PATH IS -> QUERY PUSH DOWN")
        pCatalogTablesToReplaceAsTmpTable.foreach { kv =>
          val resolvedSourceTable = resolveDataSetName(kv._1)
          val formattedProps: scala.collection.immutable.Map[String, String] = Map(CatalogProviderConfigs.CATALOG_PROVIDER ->
            sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER,
              CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER))
          val dataSetProperties: DataSetProperties =
            CatalogProvider.getDataSetProperties(resolvedSourceTable, formattedProps)
          val hiveTableParams = dataSetProperties.props
          val jdbcTableName: String = hiveTableParams(JdbcConfigs.jdbcInputTableNameKey)
          sparkSession.conf.set(JdbcConfigs.jdbcUrl, hiveTableParams(JdbcConfigs.jdbcUrl))
          sparkSession.conf.set(JdbcConfigs.jdbcDriverClassKey, hiveTableParams(JdbcConfigs.jdbcDriverClassKey))
          sqlTmpString = sqlTmpString.replaceAll(s"(?i)${kv._1}", jdbcTableName)
          sqlOriginalString = sqlOriginalString.replaceAll(s"(?i)${kv._1}", jdbcTableName)
        }
      case _ =>
        info("PATH IS --> DEFAULT")
        pCatalogTablesToReplaceAsTmpTable.foreach { kv =>
          sqlTmpString = sqlTmpString.replaceAll(s"(?i)${kv._1}", kv._2)
          sqlOriginalString = sqlOriginalString.replaceAll(s"(?i)${kv._1}", kv._2)
        }
    }

    info(s"incoming SQL --> $selectSQL")
    info(s"resolved SQL with Temp Table(s) --> $sqlTmpString")
    (sqlOriginalString, sqlTmpString, kafkaDataSets)
  }

  /**
    * Checks if a Query has Insert or if its just a select
    *
    * @param sql SQL String
    * @return true - if there is an "insert" clause, else false
    */
  def isHavingInsert(sql: String): Boolean = withMethdNameLogging { methodName =>
    val uniformSQL = sql.replace("\n", " ")
    uniformSQL.toUpperCase().contains("INSERT")
  }

  /**
    * Parse the SQL and get the entire select clause
    *
    * @param sql SQL String
    * @return SQL String - that has just the select clause
    */
  def getSelectClause(sql: String): String = withMethdNameLogging { methodName =>
    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    val selectClauseOnly = if (isHavingInsert(sql)) {
      val index = sqlParts.indexWhere(_.toUpperCase() == "SELECT")
      sqlParts.slice(index, sqlParts.length).mkString(" ")
    } else {
      sqlParts.mkString(" ")
    }
    selectClauseOnly
  }

  /**
    * Gets the target table
    *
    * @param sql SQL String
    * @return Table Name
    */
  def getTargetTables(sql: String): Option[String] = withMethdNameLogging { methodName =>
    SQLParser.getTargetTables(sql)
  }

  /**
    * getOptions - read the SparkSession options that was set by the user else add the default values
    *
    * @param sparkSession : SparkSession
    * @return Tuple ( String with concatenated options read from the SparkSession , Same Props as a Map[String, String] )
    */

  def getOptions(sparkSession: SparkSession): (String, Map[String, String]) = withMethdNameLogging { methodName =>
    val hiveConf: Map[String, String] = sparkSession.conf.getAll
    val optionsToCheck: Map[String, String] = Map(
      KafkaConfigs.rowCountOnFirstRunKey -> "250"
      , KafkaConfigs.batchFetchSize -> "250"
      , KafkaConfigs.maxRecordsPerPartition -> "25000000"
      , GimelConstants.LOG_LEVEL -> "ERROR"
      , KafkaConfigs.kafkaConsumerReadCheckpointKey -> "true"
      , KafkaConfigs.kafkaConsumerClearCheckpointKey -> "false"
      , KafkaConfigs.maxRatePerPartitionKey -> "3600"
      , KafkaConfigs.streamParallelKey -> "10"
      , KafkaConfigs.defaultBatchInterval -> "30"
      , KafkaConfigs.isStreamParallelKey -> "true"
      , KafkaConfigs.streamaWaitTerminationOrTimeoutKey -> "-1"
      , KafkaConfigs.isBackPressureEnabledKey -> "true"
      , JdbcConfigs.teradataReadType -> ""
      , HbaseConfigs.hbaseOperation -> "scan"
      , HbaseConfigs.hbaseFilter -> ""
      , GimelConstants.DATA_CACHE_IS_ENABLED -> "false"
      , GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL -> "true"
      , KafkaConfigs.isStreamBatchSwitchEnabledKey -> "false"
      , KafkaConfigs.streamFailureThresholdPerSecondKey -> "1500"
      , CatalogProviderConfigs.CATALOG_PROVIDER -> CatalogProviderConstants.PRIMARY_CATALOG_PROVIDER
    )
    val resolvedOptions: Map[String, String] = optionsToCheck.map { kvPair =>
      (kvPair._1, hiveConf.getOrElse(kvPair._1, kvPair._2))
    }
    resolvedOptions.foreach(conf => sparkSession.conf.set(conf._1, conf._2))
    (resolvedOptions.map(x => x._1 + "=" + x._2).mkString(":"), hiveConf ++ resolvedOptions)
  }


  /**
    * Executes the SQL and Returns DataFrame
    *
    * @param selectSQL    The Select SQL
    * @param sparkSession : SparkSession
    * @return DataFrame
    */
  def executeSelectClause(selectSQL: String, sparkSession: SparkSession): DataFrame = {
    val queryPushDownFlag: String = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false")
    val selectDF: DataFrame = queryPushDownFlag match {
      case "true" =>
        val df = executePushdownQuery(selectSQL, sparkSession)
        sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
        df
      case _ =>
        sparkSession.sql(selectSQL)
    }
    selectDF
  }

  /**
    * Executes the Resolved SQL Query by calling the DataSet code that has been generated
    *
    * @param clientSQL    Original SQL String submitted by Client
    * @param dest         Target Table
    * @param selectSQL    SQl String for Select Clause alone
    * @param sparkSession :SparkSession
    * @param dataset      DataSet
    * @return Result String
    */
  def executeResolvedQuery(clientSQL: String, dest: Option[String], selectSQL: String, sparkSession: SparkSession, dataset: com.paypal.gimel.DataSet): String = withMethdNameLogging { methodName =>
    info(s"Client SQL is --> $clientSQL")
    info(s"Select SQL is --> $selectSQL")
    var resultString = ""
    if (dest.isDefined) {
      info(s"EXECUTION PATH ====== DATASET WRITE ======")
      if (clientSQL.toLowerCase.contains("partition")) {
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      }
      Try {
        val options = getOptions(sparkSession)._2
        val selectDF = executeSelectClause(selectSQL, sparkSession)
        dataset.write(dest.get, selectDF, options)
      } match {
        case Success(_) =>
          resultString = "Query Completed."
          info(resultString)
        case Failure(e) =>
          e.printStackTrace()
          resultString =
            s"""Query Failed in function : ${methodName} via path dataset.write. Error -->
                |
               |${e.getStackTraceString}""".stripMargin
          error(resultString)
          throw e
      }
    } else {
      info(s"EXECUTION PATH ====== DATASET SELECT ======")
      val queryPushDownFlag: String = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false")
      val selectDF: DataFrame = queryPushDownFlag match {
        case "true" =>
          val df = executePushdownQuery(selectSQL, sparkSession)
          sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
          df
        case _ =>
          sparkSession.sql(selectSQL)
      }
      val count = selectDF.cache.count
      val rowsToShow = sparkSession.conf.get(GimelConstants.MAX_RESULTS_TO_SHOW, "250").toInt
      val showRowsOnly = sparkSession.conf.get(GimelConstants.SHOW_ROWS_ENABLED, "false").toBoolean
      val resultSet = selectDF.take(rowsToShow).mkString("\n")
      val marginString = "-------------------------------------------------------------------------------------------------------"
      val extraMessage =
        s"""
           |$marginString
           |Total Rows Returned from original Query --> $count
           |Displaying Rows ${scala.math.min(rowsToShow, count)} of $count
           |
           |$userInfoString
        """.stripMargin
      resultString =
        s"""${if (!showRowsOnly) extraMessage else ""}
           |$marginString
           |$resultSet
           |$marginString""".stripMargin
    }
    resultString
  }

  /**
    * Executes the Resolved SQL Query by calling the DataSet code that has been generated
    *
    * @param clientSQL    Original SQL String submitted by Client
    * @param dest         Target Table
    * @param selectSQL    SQl String for Select Clause alone
    * @param sparkSession : SparkSession
    * @param dataset      DataSet
    * @return RDD[Result JSON String]
    */
  //  def executeResolvedQuerySparkMagic(clientSQL: String, dest: Option[String], selectSQL: String, hiveContext: HiveContext, dataset: DataSet): RDD[String] = withMethdNameLogging { methodName =>
  //    info(s"Client SQL is --> $clientSQL")
  //    info(s"Select SQL is --> $selectSQL")
  //    silence
  //    val selectDF = hiveContext.sql(selectSQL)
  //    selectDF.toJSON
  //  }

  def executeResolvedQuerySparkMagic(clientSQL: String, dest: Option[String], selectSQL: String, sparkSession: SparkSession, dataset: com.paypal.gimel.DataSet): RDD[String] = withMethdNameLogging { methodName =>
    info(s"Client SQL is --> $clientSQL")
    info(s"Select SQL is --> $selectSQL")
    var resultString = ""
    if (dest.isDefined) {
      info(s"EXECUTION PATH ====== DATASET WRITE ======")
      if (clientSQL.toLowerCase.contains("partition")) {
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict")
      }
      Try {
        val (_, options) = getOptions(sparkSession)
        val queryPushDownFlag: String = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false")
        val selectDF: DataFrame = queryPushDownFlag match {
          case "true" =>
            val df = executePushdownQuery(selectSQL, sparkSession)
            sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
            df
          case _ =>
            sparkSession.sql(selectSQL)
        }
        dataset.write(dest.get, selectDF, options)
      } match {
        case Success(_) =>
          resultString = """{"Query Execution":"Success"}"""
          info(resultString)
          sparkSession.read.json(sparkSession.sparkContext.parallelize(Seq(resultString))).toJSON.rdd
        case Failure(e) =>
          e.printStackTrace()
          resultString =
            s"""{"Query Execution Failed":${e.getStackTraceString}}"""
          error(resultString)
          sparkSession.read.json(sparkSession.sparkContext.parallelize(Seq(resultString))).toJSON.rdd
        //          throw e
      }
    } else {
      info(s"EXECUTION PATH ====== DATASET SELECT ======")
      val queryPushDownFlag: String = sparkSession.conf.get(JdbcConfigs.jdbcPushDownEnabled, "false")
      val selectDF: DataFrame = queryPushDownFlag match {
        case "true" =>
          val df = executePushdownQuery(selectSQL, sparkSession)
          sparkSession.conf.set(JdbcConfigs.jdbcPushDownEnabled, "false")
          df
        case _ =>
          sparkSession.sql(selectSQL)
      }
      //      val count = selectDF.cache.count
      val rowsToShow = sparkSession.conf.get(GimelConstants.MAX_RESULTS_TO_SHOW, "250").toInt
      selectDF.registerTempTable("tmp_table_spark_magic")
      val resultSet = sparkSession.sql(s"select * from tmp_table_spark_magic limit ${rowsToShow}").toJSON.rdd
      resultSet
    }
  }

  /**
    * Resolves the Query by replacing Tmp Tables in the Query String
    * For Each Tmp Table placed in the Query String - a DataSet.read is initiated
    * For each Tmp Table - if the dataset is a Kafka DataSet - then each KafkaDataSet object is accumulated
    * Accumulated KafkaDataSet Object will be used towards the end of the Query (on success) -
    * to call check pointing for each topic consumed
    *
    * @param sql          SQL String
    * @param sparkSession : SparkSession
    * @param dataSet      DataSet
    * @return Tuple ( Target Table, select SQL String, List(KafkaDataSet)  )
    */
  def resolveSQL(sql: String, sparkSession: SparkSession, dataSet: com.paypal.gimel.DataSet): (String, Option[String], String, List[com.paypal.gimel.kafka.DataSet]) = withMethdNameLogging { methodName =>
    info(s"incoming SQL --> $sql")
    val uniformSQL = sql.replace("\n", " ")
    val selectClauseOnly = getSelectClause(uniformSQL)
    val (originalSQL, selectClause, kafkaDataSets) = resolveSQLWithTmpTables(sql, selectClauseOnly, sparkSession, dataSet)
    val targetTable = getTargetTables(sql)
    info(s"selectClause --> $selectClause")
    info(s"destination --> $targetTable")
    (originalSQL, targetTable, selectClause, kafkaDataSets)
  }

  private lazy val userInfoString =
    s"""
       |------------------------------
       |User controllable Properties
       |------------------------------
       |
       |Query Results & Helper
       |----------------------
       |${GimelConstants.SHOW_ROWS_ENABLED} --> Set this to "true" to stop getting all these messages. (Default : false)
       |${GimelConstants.MAX_RESULTS_TO_SHOW} --> Number of rows to display in interactive mode (Default : 1000)
       |
       |Data Caching Options
       |----------------------
       |${GimelConstants.DATA_CACHE_IS_ENABLED} --> true indicates dataset caching is enabled (Default : false)
       |${GimelConstants.DATA_CACHE_IS_ENABLED}.for.pcatalog.flights --> if this = true & ${GimelConstants.DATA_CACHE_IS_ENABLED}=true, then only pcatalog.flights from query will be cached. (Default : false)
       |${GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL} --> if this = true, then all pcatalog datasets in query will be cached (Default : true)
       |
       |Logging Level
       |----------------------
       |${GimelConstants.LOG_LEVEL} --> set to INFO, DEBUG, WARN, ERROR to get desired level of logging (Default : ERROR)
       |
       |kafka Checkpointing
       |----------------------
       |${KafkaConfigs.kafkaConsumerReadCheckpointKey} --> true indicates check-pointing enabled (Default : true)
       |${KafkaConfigs.kafkaConsumerClearCheckpointKey} --> true indicates checkpoint will be cleared before this run begins (Default : false)
       |
       |kafka Stream Throttle
       |----------------------
       |${KafkaConfigs.maxRatePerPartitionKey} --> Spark Configuration for Streaming Rate (Default : 3600, empirically derived)
       |${KafkaConfigs.isStreamParallelKey} --> true causes ordering to be lost, but performance gain via parallelism factor. (Default : true)
       |${KafkaConfigs.streamParallelKey} --> Number of parallel threads to run while processing data after fetching from kafka (Default : 10)
       |${KafkaConfigs.defaultBatchInterval} --> Streaming Window Seconds (Default : 30)
       |
       |kafka Batch Throttle
       |----------------------
       |${KafkaConfigs.rowCountOnFirstRunKey} --> Fetches Only Supplied number of rows from Kafka (Default : 25 Million)
       |${KafkaConfigs.maxRecordsPerPartition} --> Advanced options to further restrict how many messages we can read from each partition - in batch mode Kafka Read (Default 25 Million rows, for this to be effective, value should be <= throttle.batch.fetchRowsOnFirstRun)
       |${KafkaConfigs.batchFetchSize} --> Advanced options to parallelize in batch mode Kafka Read (Default 250) --> This will parallelize 25 Million into 250 threads
       |
       |HBase
       |-----------------------
       |${HbaseConfigs.hbaseOperation} -> Type of operation to be performed on HBase. Can be scan for reading all data or get for lookup
       |${HbaseConfigs.hbaseFilter} -> Filter condition for HBase lookup. Example: rowKey=1:toGet=cf1-c1,c2|cf2-c3
       |
       |Elastic
       |-----------------------
       |${ElasticSearchConfigs.esIsPartitioned}-> Is the index partitioned or not ?
       |${ElasticSearchConfigs.esDelimiter}-> What is the delimiter which separates the index name with the partition
       |${ElasticSearchConfigs.esPartition}-> "*" -> wild card to include all the specific partitions
       |${ElasticSearchConfigs.esDefaultReadForAllPartitions}-> flag which indicates whether to read all partitions or not
       |${ElasticSearchConfigs.esMapping}-> flag which gets the schema from the user
     """.stripMargin
}

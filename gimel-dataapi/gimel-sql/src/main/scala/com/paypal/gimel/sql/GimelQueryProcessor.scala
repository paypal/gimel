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

import scala.collection.immutable.Map
import scala.util.{Failure, Success, Try}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.StructField
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel._
import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.{CatalogProviderConfigs, GimelConstants}
import com.paypal.gimel.common.gimelserde.GimelSerdeUtils
import com.paypal.gimel.common.utilities.{DataSetType, DataSetUtils, GenericUtils, Timer}
import com.paypal.gimel.datasetfactory.GimelDataSet
import com.paypal.gimel.datastreamfactory.{StreamingResult, StructuredStreamingResult, WrappedData}
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.logging.GimelStreamingListener
import com.paypal.gimel.parser.utilities.{QueryConstants, QueryParserUtils}

object GimelQueryProcessor {

  val logger: Logger = Logger(this.getClass.getName)
  lazy val pCatalogStreamingKafkaTmpTableName = "pcatalog_streaming_kafka_tmp_table"
  val queryUtils = GimelQueryUtils

  import queryUtils._

  val originalUser = sys.env("USER")
  var user = originalUser
  var isQueryFromGTS = false
  val yarnCluster = com.paypal.gimel.common.utilities.DataSetUtils.getYarnClusterName()

  /**
    * At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
    *
    * @param sparkSession Spark Session
    */
  def setCatalogProviderInfo(sparkSession: SparkSession): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val catalogProvider: String = sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER, GimelConstants.UDC_STRING)
    val catalogProviderName: String = sparkSession.conf.get(CatalogProviderConfigs.CATALOG_PROVIDER_NAME_SPACE, GimelConstants.UDC_STRING)
    logger.info(s"Catalog Provider --> [${catalogProvider}] | Catalog Provider Name --> [${catalogProviderName}] ")
    setCatalogProvider(catalogProvider)
    setCatalogProviderName(catalogProviderName)
  }

  /**
   * Sets Spark GTS User Name if available
   *
   * @param sparkSession SparkSession
   */
  def setGtsUser(sparkSession: SparkSession): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val gtsUser: String = sparkSession.sparkContext.getLocalProperty(GimelConstants.GTS_USER_CONFIG)
    val gts_default_user = sparkSession.conf.get(GimelConstants.GTS_DEFAULT_USER_FLAG, "")
    if (gtsUser != null && originalUser.equalsIgnoreCase(gts_default_user)) {
      logger.info(s"GTS User [${gtsUser}] will be used to over ride executing user [${originalUser}] who started GTS.")
      sparkSession.sql(s"set ${GimelConstants.GTS_USER_CONFIG}=${gtsUser}")

      // set jdbc username,if already not set in sparkconf
      val jdbcUser: Option[String] = sparkSession.conf.getOption(JdbcConfigs.jdbcUserName)
      if (jdbcUser.isEmpty) {
        logger.info(s"Setting ${JdbcConfigs.jdbcUserName}=${gtsUser}")
        sparkSession.sql(s"set ${JdbcConfigs.jdbcUserName}=${gtsUser}")
      }
      user = gtsUser
      isQueryFromGTS = true
    }
  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */
  def executeBatch(sql: String, sparkSession: SparkSession): DataFrame = {

    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val uniformSQL = sql.replace("\n", " ").trim
    val sqlArray: Array[String] = uniformSQL.split(";")
    val totalStatements = sqlArray.length
    val dataFrames: Array[DataFrame] = sqlArray.zipWithIndex.map(eachSql => {
      val sqlString = eachSql._1
      val index = eachSql._2
      logger.info(s"Executing statement: ${sqlString}")
      try {
        executeBatchStatement(sqlString, sparkSession)
      }
      catch {
        case e: Throwable =>
          val errorMsg =
            s"""
               | Statements[${index}/${totalStatements}] successfully executed.
               | Statement[${index + 1}] execution failed --> ${sqlString}
            """.stripMargin
          logger.throwError(s"${errorMsg}")
          throw e
      }
    })
    logger.info(s"${totalStatements}/${totalStatements} statements successfully executed.")
    dataFrames(totalStatements - 1)
  }

  /**
    * This method will process one statement from executebatch
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */
  def executeBatchStatement(sql: String, sparkSession: SparkSession): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
    logger.info(" @Begin --> " + MethodName)

    logger.setSparkVersion(sparkSession.version)

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    val sparkAppName = sparkSession.conf.get("spark.app.name")

    try {

      // At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
      setCatalogProviderInfo(sparkSession)

      // If query comes from GTS - interpret the GTS user and set it
      setGtsUser(sparkSession)

      val options = queryUtils.getOptions(sparkSession)._2

      var resultingString = ""
      val queryTimer = Timer()
      //      val startTime = queryTimer.start
      val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      //      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean

      logger.debug(s"Is CheckPointing Requested By User --> $isCheckPointEnabled")
      val dataSet: DataSet = DataSet(sparkSession)

      // Identify JDBC complete pushdown
      val (isJdbcCompletePushDownEnabled, transformedSql, jdbcOptions) =
        GimelQueryUtils.isJdbcCompletePushDownEnabled(sparkSession, sql)

      val data = if (isJdbcCompletePushDownEnabled) {
        GimelQueryUtils.createPushDownQueryDataframe(sparkSession, transformedSql.get, jdbcOptions.get)
      } else if (queryUtils.isUDCDataDefinition(sql)) {
        logger.info("This path is dynamic dataset creation path")
        var resultingStr = ""
        Try(
          handleDDLs(sql, sparkSession, dataSet, options)
        ) match {
          case Success(result) =>
            resultingStr = "Query Completed."
          case Failure(e) =>
            resultingStr = s"Query Failed in function : $MethodName. Error --> \n\n ${
              e.toString
            }"
            logger.error(resultingStr)
            throw e
        }
        stringToDF(sparkSession, resultingStr)
      } else {

        // Set HBase Page Size for optimization if selecting from HBase with limit
        if (QueryParserUtils.isHavingLimit(sql)) {
          setLimitForHBase(sql, options, sparkSession)
        }

        val (originalSQL, destination, selectSQL, kafkaDataSets, queryPushDownFlag) =
          resolveSQL(sql, sparkSession, dataSet)
        destination match {
          case Some(target) =>
            logger.info(s"Target Exists --> ${target}")
            Try(
              executeResolvedQuery(originalSQL, destination, selectSQL, sparkSession, dataSet, queryPushDownFlag)
            ) match {
              case Success(result) =>
                resultingString = result
              case Failure(e) =>
                resultingString = s"Query Failed in function : $MethodName. Error --> \n\n ${
                  e.toString
                }"
                logger.error(resultingString)
                throw e
            }

            if (isCheckPointEnabled) {
              saveCheckPointforKafka(kafkaDataSets)
            }
            import sparkSession.implicits._
            Seq(resultingString).toDF("Query Execution")

          case _ =>
            logger.info(s"No Target, returning DataFrame back to client.")
            executeSelectClause(selectSQL, sparkSession, queryPushDownFlag)
        }
      }

      // pushing logs to ES
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeBatch
        , yarnCluster
        , user
        , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
        , MethodName
        , sql
        , scala.collection.mutable.Map("sql" -> sql, "isQueryFromGTS" -> isQueryFromGTS.toString, "originalUser" -> originalUser)
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
      )

      data

    } catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeBatch
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql, "isQueryFromGTS" -> isQueryFromGTS.toString, "originalUser" -> originalUser)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw new Exception(s"${e.getMessage}\n", e)
    } finally {
      logger.info("Unsetting the property -> " + GimelConstants.HBASE_PAGE_SIZE)
      sparkSession.conf.unset(GimelConstants.HBASE_PAGE_SIZE)
    }
  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    * Executes the executeBatch function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  def executeStream(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.setSparkVersion(sparkSession.version)
    val sparkAppName = sparkSession.conf.get("spark.app.name")

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    // At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
    setCatalogProviderInfo(sparkSession)

    try {

      sparkSession.conf.set(GimelConstants.GIMEL_KAFKA_VERSION, GimelConstants.GIMEL_KAFKA_VERSION_ONE)
      val options = queryUtils.getOptions(sparkSession)._2
      val batchInterval = options(KafkaConfigs.defaultBatchInterval).toInt
      val streamRate = options(KafkaConfigs.maxRatePerPartitionKey)
      val isBackPressureEnabled = options(KafkaConfigs.isBackPressureEnabledKey)
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      val isSaveCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      val isStreamFailureBeyondThreshold = options.getOrElse(KafkaConfigs.isStreamBatchSwitchEnabledKey, "false").toBoolean
      val streamFailureThresholdPerSecond = options.getOrElse(KafkaConfigs.failStreamThresholdKey, "1200").toInt
      val streamFailureWindowFactor = options.getOrElse(KafkaConfigs.streamFailureWindowFactorKey, "10").toString.toInt
      val isStreamParallel = options(KafkaConfigs.isStreamParallelKey)
      val streamParallels = options(KafkaConfigs.streamParallelKey)
      val streamawaitTerminationOrTimeout = options(KafkaConfigs.streamaWaitTerminationOrTimeoutKey).toLong
      val sc = sparkSession.sparkContext
      val sqlContext = sparkSession.sqlContext
      val conf = new org.apache.spark.SparkConf()
      val ssc = new StreamingContext(sc, Seconds(batchInterval))
      val listner: GimelStreamingListener = new GimelStreamingListener(conf)
      ssc.addStreamingListener(listner)
      logger.debug(
        s"""
           |isStreamParallel --> $isStreamParallel
           |streamParallels --> $streamParallels
      """.stripMargin)
      ssc.sparkContext.getConf
        .set(KafkaConfigs.isBackPressureEnabledKey, isBackPressureEnabled)
        .set(KafkaConfigs.streamMaxRatePerPartitionKey, streamRate)
        .set(KafkaConfigs.isStreamParallelKey, isStreamParallel)
        .set(KafkaConfigs.streamParallelKey, streamParallels)
      val dataStream = DataStream(ssc)
      val sourceTables = getTablesFrom(sql)
      val kafkaTables = sourceTables.filter { table =>
        DataSetUtils.getSystemType(table, sparkSession, options) == DataSetType.KAFKA
      }
      if (kafkaTables.isEmpty) {
        throw new Exception("ERROR --> No Kafka Type DataSet In the Query To Stream !")
      } else {
        val tmpKafkaTable = pCatalogStreamingKafkaTmpTableName
        val newSQL = sql.replaceAll(kafkaTables.head, tmpKafkaTable)
        val streamingResult: StreamingResult = dataStream.read(kafkaTables.head, options)
        if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint As Requested By User")
        try {
          streamingResult.dStream.foreachRDD { (rdd, time) =>
            printStats(time, listner)
            val count = rdd.count()
            if (count > 0) {
              if (isStreamFailureBeyondThreshold) {
                if ((count / batchInterval) > streamFailureThresholdPerSecond) throw new Exception(s"Current Messages Per Second : ${count / batchInterval} exceeded Supplied Stream Capacity ${streamFailureThresholdPerSecond}")
                else logger.info(s"Current Messages Per Second : ${count / batchInterval} within Supplied Stream Capacity ${streamFailureThresholdPerSecond}")
              }
              val failureThreshold = (batchInterval * streamFailureWindowFactor)
              val totalDelay = (listner.totalDelay / 1000)
              if (totalDelay > failureThreshold) {
                throw new Exception(
                  s"""Current Total_Delay:$totalDelay exceeded $failureThreshold <MultiplicationFactor:$streamFailureWindowFactor X StreamingWindow:$batchInterval>
If mode=intelligent, then Restarting will result in Batch Mode Execution first for catchup, and automatically migrate to stream mode !
                   """.stripMargin
                )
              } else logger.info(s"Current Total_Delay:$totalDelay within $failureThreshold <MultiplicationFactor:$streamFailureWindowFactor X StreamingWindow:$batchInterval>")
              streamingResult.getCurrentCheckPoint(rdd)
              streamingResult.getAsDF(sqlContext, rdd).registerTempTable(tmpKafkaTable)
              try {
                executeBatch(newSQL, sparkSession)
              } catch {
                case ex: Throwable =>
                  // logger.error(s"Stream Query Failed in function : $MethodName. Error --> \n\n${ex.getStackTraceString}")
                  // ex.printStackTrace()
                  // logger.error("Force - Stopping Streaming Context")
                  ssc.sparkContext.stop()
                  throw ex
              }
              try {
                if (isSaveCheckPointEnabled) streamingResult.saveCurrentCheckPoint()
                if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint as Requested by User")
              }
              catch {
                case ex: Throwable =>
                  // logger.error("Error in CheckPoint Operations in Streaming.")
                  // ex.printStackTrace()
                  ssc.sparkContext.stop()
              }
            }
          }
        } catch {
          case ex: Throwable =>
            // logger.error(s"ERROR In Streaming Window --> \n\n${ex.getStackTraceString}")
            // ex.printStackTrace()
            ssc.sparkContext.stop()
            throw ex
        }
        dataStream.streamingContext.start()
        dataStream.streamingContext.awaitTerminationOrTimeout(streamawaitTerminationOrTimeout)
        dataStream.streamingContext.stop(false, true)

        // push to logger
        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.SUCCESS
          , GimelConstants.EMPTY_STRING
          , GimelConstants.EMPTY_STRING
        )
        "Success"
      }

    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw e
    }

  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    * Executes the executeBatch function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  def executeStream2(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.setSparkVersion(sparkSession.version)
    val sparkAppName = sparkSession.conf.get("spark.app.name")

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    // At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
    setCatalogProviderInfo(sparkSession)

    try {

      val options = queryUtils.getOptions(sparkSession)._2
      val batchInterval = options(KafkaConfigs.defaultBatchInterval).toInt
      val triggerInterval = options.getOrElse(GimelConstants.GIMEL_STREAMING_TRIGGER_INTERVAL, "").toString
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      val sc = sparkSession.sparkContext
      val conf = new org.apache.spark.SparkConf()
      val ssc = new StreamingContext(sc, Seconds(batchInterval))
      val listener: GimelStreamingListener = new GimelStreamingListener(conf)
      ssc.addStreamingListener(listener)
      val dataStream = DataStream2(sparkSession)
      val sourceTables = getTablesFrom(sql)
      val targetTable = getTargetTables(sql)
      val kafkaTables = sourceTables.filter { table =>
        val dataSetType = DataSetUtils.getSystemType(table, sparkSession, options)
        (dataSetType == DataSetType.KAFKA || dataSetType == DataSetType.KAFKA2)
      }
      if (kafkaTables.isEmpty) {
        throw new Exception("ERROR --> No Kafka Type DataSet In the Query To Stream !")
      } else {
        val tmpKafkaTable = pCatalogStreamingKafkaTmpTableName
        val selectSQL = getSelectClause(sql)
        val newSQL = selectSQL.toLowerCase().replaceAll(kafkaTables.head, tmpKafkaTable)
        val datasetProps = CatalogProvider.getDataSetProperties(kafkaTables.head, options)
        /*
         * Sets the appropriate deserializer class based on the kafka.message.value.type and value.serializer properties
         * This is mainly required for backward compatibility for KAFKA datasets
         */
        val newOptions = GimelSerdeUtils.setGimelDeserializer(sparkSession, datasetProps, options, true)
        val streamingResult: StructuredStreamingResult = dataStream.read(kafkaTables.head, newOptions)
        val streamingDF = streamingResult.df
        streamingDF.createOrReplaceTempView(tmpKafkaTable)

        val streamingSQLDF = sparkSession.sql(newSQL)
        var writer: StreamingQuery = null
        try {
          val datastreamWriter = targetTable match {
            case Some(target) =>
              val datasetProps = CatalogProvider.getDataSetProperties(target, options)
              /*
               * Sets the appropriate serializer class based on the kafka.message.value.type and value.serializer properties
               * This is mainly required for backward compatibility for KAFKA datasets
              */
              val newOptions = GimelSerdeUtils.setGimelSerializer(sparkSession, datasetProps, options, true)
              dataStream.write(target, streamingSQLDF, newOptions)
            case _ =>
              streamingSQLDF
                .writeStream
                .outputMode("append")
                .format("console")
          }

          writer = if (triggerInterval.isEmpty) {
            datastreamWriter.start()
          } else {
            datastreamWriter
              .trigger(Trigger.ProcessingTime(triggerInterval + " seconds"))
              .start()
          }

        } catch {
          case ex: Throwable =>
            // logger.error(s"ERROR In Streaming Window --> \n\n${ex.getStackTraceString}")
            // ex.printStackTrace()
            if (writer != null) {
              writer.stop
            }
            throw ex
        }

        // push to logger
        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.SUCCESS
          , GimelConstants.EMPTY_STRING
          , GimelConstants.EMPTY_STRING
        )
        "Success"
      }

    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw e
    }

  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    *
    * @return RDD[Resulting String < either sample data for select queries, or "success" / "failed" for insert queries]
    */
  def executeBatchSparkMagic: (String, SparkSession) => RDD[String] = executeBatchSparkMagicRDD

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    * Executes the executeBatchSparkMagicRDD function in streaming window
    *
    * @return RDD[Resulting String] < either sample data for select queries, or "success" / "failed" for insert queries
    */
  def executeStreamSparkMagic: (String, SparkSession) => RDD[String] = executeStreamSparkMagicRDD

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return RDD[Resulting String < either sample data for select queries, or "success" / "failed" for insert queries]
    */
  def executeBatchSparkMagicRDD(sql: String, sparkSession: SparkSession): RDD[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.setSparkVersion(sparkSession.version)

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    val sparkAppName = sparkSession.conf.get("spark.app.name")

    // At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
    setCatalogProviderInfo(sparkSession)

    try {

      val options = queryUtils.getOptions(sparkSession)._2

      var resultingRDD: RDD[String] = sparkSession.sparkContext.parallelize(Seq(""))
      val queryTimer = Timer()
      val startTime = queryTimer.start
      val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      logger.debug(s"Is CheckPointing Requested By User --> ${
        isCheckPointEnabled
      }")
      val dataSet: DataSet = DataSet(sparkSession)
      val (originalSQL, destination, selectSQL, kafkaDataSets, queryPushDownFlag) = resolveSQL(sql, sparkSession, dataSet)
      Try(executeResolvedQuerySparkMagic(originalSQL, destination, selectSQL, sparkSession, dataSet, queryPushDownFlag)) match {
        case Success(result) =>
          resultingRDD = result
        case Failure(e) =>
          resultingRDD = sparkSession.sparkContext.parallelize(Seq(
            s"""{"Batch Query Error" : "${
              e.getStackTraceString
            }" """))
          val resultMsg = resultingRDD.collect().mkString("\n")
          // logger.error(resultMsg)
          throw new Exception(resultMsg)
      }
      if (isCheckPointEnabled) {
        saveCheckPointforKafka(kafkaDataSets)
      }
      if (isClearCheckPointEnabled) {
        clearCheckPointforKafka(kafkaDataSets)
      }

      // push logs to ES
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeStream
        , yarnCluster
        , user
        , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
        , MethodName
        , sql
        , scala.collection.mutable.Map("sql" -> sql)
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
      )
      resultingRDD
    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )
        // throw error to console
        logger.throwError(e.toString)

        throw e
    }
  }


  def saveCheckPointforKafka(kafkaDataSets: List[GimelDataSet]): Unit = {
    kafkaDataSets.foreach {
      case kafka: com.paypal.gimel.kafka.DataSet =>
        kafka.saveCheckPoint()
      case kafka2: com.paypal.gimel.kafka2.DataSet =>
        kafka2.saveCheckPoint()
    }

  }


  def clearCheckPointforKafka(kafkaDataSets: List[GimelDataSet]): Unit = {
    kafkaDataSets.foreach {
      case kafka: com.paypal.gimel.kafka.DataSet =>
        kafka.clearCheckPoint()
      case kafka2: com.paypal.gimel.kafka2.DataSet =>
        kafka2.clearCheckPoint()
    }
  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    * Executes the executeBatchSparkMagicRDD function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return RDD[Resulting String] < either sample data for select queries, or "success" / "failed" for insert queries
    */

  def executeStreamSparkMagicRDD(sql: String, sparkSession: SparkSession): RDD[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.setSparkVersion(sparkSession.version)

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    val sparkAppName = sparkSession.conf.get("spark.app.name")

    // At Run Time - Set the Catalog Provider and The Name Space of the Catalog (like the Hive DB Name when catalog Provider = HIVE)
    setCatalogProviderInfo(sparkSession)

    try {

      sparkSession.conf.set(GimelConstants.GIMEL_KAFKA_VERSION, GimelConstants.GIMEL_KAFKA_VERSION_ONE)
      val options = getOptions(sparkSession)._2

      val batchInterval = options(KafkaConfigs.defaultBatchInterval).toInt
      val streamRate = options(KafkaConfigs.maxRatePerPartitionKey)
      val isBackPressureEnabled = options(KafkaConfigs.isBackPressureEnabledKey)
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      val isSaveCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      val isStreamParallel = options(KafkaConfigs.isStreamParallelKey)
      val sc = sparkSession.sparkContext
      val sqlContext = sparkSession.sqlContext
      val ssc = new StreamingContext(sc, Seconds(batchInterval))
      val listner: GimelStreamingListener = new GimelStreamingListener(sc.getConf)
      ssc.addStreamingListener(listner)
      ssc.sparkContext.getConf
        .set(KafkaConfigs.isBackPressureEnabledKey, isBackPressureEnabled)
        .set(KafkaConfigs.streamMaxRatePerPartitionKey, streamRate)
        .set(KafkaConfigs.isStreamParallelKey, isStreamParallel)
      val dataStream = DataStream(ssc)
      val sourceTables = getTablesFrom(sql)
      val kafkaTables = sourceTables.filter { table =>
        val dataSetProperties: DataSetProperties =
          CatalogProvider.getDataSetProperties(table, options)
        DataSetUtils.getSystemType(dataSetProperties) == DataSetType.KAFKA
      }
      val data = if (kafkaTables.isEmpty) {
        throw new Exception("ERROR --> No Kafka Type DataSet In the Query To Stream !")
      } else {
        try {
          val tmpKafkaTable = pCatalogStreamingKafkaTmpTableName
          val newSQL = sql.replaceAll(kafkaTables.head, tmpKafkaTable)
          val streamingResult: StreamingResult = dataStream.read(kafkaTables.head, options)
          if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint As Requested By User")
          streamingResult.dStream.foreachRDD {
            (rdd, time) =>
              printStats(time, listner)
              val k: RDD[WrappedData] = rdd
              val count = rdd.count()
              if (count > 0) {
                streamingResult.getCurrentCheckPoint(rdd)
                streamingResult.getAsDF(sqlContext, rdd).registerTempTable(tmpKafkaTable)
                try {
                  executeBatchSparkMagicRDD(newSQL, sparkSession)
                }
                catch {
                  case ex: Throwable =>
                    // logger.error(s"Stream Query Failed in function : $MethodName. Error --> \n\n${ex.getStackTraceString}")
                    // ex.printStackTrace()
                    // logger.error("Force - Stopping Streaming Context")
                    ssc.sparkContext.stop()
                }
                try {
                  if (isSaveCheckPointEnabled) streamingResult.saveCurrentCheckPoint()
                  if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint as Requested by User")
                }
                catch {
                  case ex: Throwable =>
                    // logger.error("Error in CheckPoint Operations in Streaming.")
                    // ex.printStackTrace()
                    ssc.sparkContext.stop()
                }
              }
          }
          dataStream.streamingContext.start()
          dataStream.streamingContext.awaitTermination()
          dataStream.streamingContext.sparkContext.parallelize(Seq(s"""{"Query" : "Running..." }"""))
        } catch {
          case ex: Throwable =>
            ex.printStackTrace()
            val msg =
              s"""{"Error" : "${
                ex.getStackTraceString
              }" }"""
            dataStream.streamingContext.stop()
            //            dataStream.streamingContext.spark.parallelize(Seq(s"""{"Error" : "${ex.getStackTraceString}" }"""))
            throw new Exception(msg)
        }
      }

      // push logs to ES
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeStream
        , yarnCluster
        , user
        , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
        , MethodName
        , sql
        , scala.collection.mutable.Map("sql" -> sql)
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
      )

      data
    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw e
    }

  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  @deprecated
  def executeBatchSparkMagicJSON(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val sparkAppName = sparkSession.conf.get("spark.app.name")

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    try {
      val options = queryUtils.getOptions(sparkSession)._2
      var resultSet = ""
      val queryTimer = Timer()
      val startTime = queryTimer.start
      val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      logger.debug(s"Is CheckPointing Requested By User --> ${
        isCheckPointEnabled
      }")
      val dataSet: DataSet = DataSet(sparkSession)
      val (originalSQL, destination, selectSQL, kafkaDataSets, queryPushDownFlag) = resolveSQL(sql, sparkSession, dataSet)
      Try(executeResolvedQuerySparkMagic(originalSQL, destination, selectSQL, sparkSession, dataSet, queryPushDownFlag)) match {
        case Success(result) =>
          resultSet =
            s"""{"Batch Query Result" : "${
              result.collect().mkString("[", ",", "]")
            } }"""
        case Failure(e) =>
          resultSet =
            s"""{"Batch Query Error" : "${
              e.getStackTraceString
            }" """
          // logger.error(resultSet)
          throw new Exception(resultSet)
      }

      if (isCheckPointEnabled) {
        saveCheckPointforKafka(kafkaDataSets)
      }
      if (isClearCheckPointEnabled) {
        clearCheckPointforKafka(kafkaDataSets)
      }

      // push logs to ES
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeStream
        , yarnCluster
        , user
        , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
        , MethodName
        , sql
        , scala.collection.mutable.Map("sql" -> sql)
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
      )
      resultSet
    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw e
    }

  }

  /**
    * Core Function that will be called from SCAAS for executing a SQL
    * Executes the @executeBatchSparkMagicJSON function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  @deprecated
  def executeStreamSparkMagicJSON(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    var returnMsg = ""

    // Set gimel log level and flag to audit logs to kafka
    DataSetUtils.setGimelLogLevel(sparkSession, logger)

    try {
      sparkSession.conf.set(GimelConstants.GIMEL_KAFKA_VERSION, GimelConstants.GIMEL_KAFKA_VERSION_ONE)
      val options = queryUtils.getOptions(sparkSession)._2
      val batchInterval = options(KafkaConfigs.defaultBatchInterval).toInt
      val streamRate = options(KafkaConfigs.maxRatePerPartitionKey)
      val isBackPressureEnabled = options(KafkaConfigs.isBackPressureEnabledKey)
      val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
      val isSaveCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
      val isStreamParallel = options(KafkaConfigs.isStreamParallelKey)
      val streamParallels = options(KafkaConfigs.streamParallelKey)
      val sc = sparkSession.sparkContext
      val sqlContext = sparkSession.sqlContext
      val ssc = new StreamingContext(sc, Seconds(batchInterval))
      logger.debug(
        s"""
           |isStreamParallel --> ${
          isStreamParallel
        }
           |streamParallels --> ${
          streamParallels
        }
      """.stripMargin)
      ssc.sparkContext.getConf
        .set(KafkaConfigs.isBackPressureEnabledKey, isBackPressureEnabled)
        .set(KafkaConfigs.streamMaxRatePerPartitionKey, streamRate)
        .set(KafkaConfigs.isStreamParallelKey, isStreamParallel)
        .set(KafkaConfigs.streamParallelKey, streamParallels)
      val dataStream = DataStream(ssc)
      val sourceTables = getTablesFrom(sql)
      val kafkaTables = sourceTables.filter { table =>
        val dataSetProperties: DataSetProperties =
          CatalogProvider.getDataSetProperties(table, options)
        DataSetUtils.getSystemType(dataSetProperties) == DataSetType.KAFKA
      }
      if (kafkaTables.isEmpty) {
        throw new Exception("ERROR --> No Kafka Type DataSet In the Query To Stream !")
      } else {
        val tmpKafkaTable = pCatalogStreamingKafkaTmpTableName
        val newSQL = sql.replaceAll(kafkaTables.head, tmpKafkaTable)
        val streamingResult: StreamingResult = dataStream.read(kafkaTables.head, options)
        if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint As Requested By User")
        try {
          streamingResult.dStream.foreachRDD {
            rdd =>
              val k: RDD[WrappedData] = rdd
              val count = rdd.count()
              if (count > 0) {
                streamingResult.getCurrentCheckPoint(rdd)
                streamingResult.convertAvroToDF(sqlContext, streamingResult.convertBytesToAvro(rdd)).registerTempTable(tmpKafkaTable)
                try {
                  executeBatchSparkMagicJSON(newSQL, sparkSession)
                  if (isSaveCheckPointEnabled) streamingResult.saveCurrentCheckPoint()
                  if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint as Requested by User")
                } catch {
                  case ex: Throwable =>
                    returnMsg =
                      s"""{ "Stream Query Error" : "${
                        ex.getStackTraceString
                      }" } """
                    // logger.error(returnMsg)
                    // ex.printStackTrace()
                    logger.warning("Force - Stopping Streaming Context")
                    ssc.sparkContext.stop()
                    throw new Exception(returnMsg)
                }
              }
          }
        } catch {
          case ex: Throwable =>
            returnMsg =
              s"""{ "Stream Query ERROR" : "${
                ex.getStackTraceString
              }" } """
            // logger.error(returnMsg)
            // ex.printStackTrace()
            logger.warning("Force - Stopping Streaming Context")
            ssc.sparkContext.stop()
            throw new Exception(returnMsg)
        }
        dataStream.streamingContext.start()
        dataStream.streamingContext.awaitTermination()
        dataStream.streamingContext.stop()
        returnMsg = s"""{"Stream Query" : "SUCCESS"} """
      }

      // push logs to ES
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , KafkaConstants.gimelAuditRunTypeStream
        , yarnCluster
        , user
        , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
        , MethodName
        , sql
        , scala.collection.mutable.Map("sql" -> sql)
        , GimelConstants.SUCCESS
        , GimelConstants.EMPTY_STRING
        , GimelConstants.EMPTY_STRING
      )

      returnMsg
    }
    catch {
      case e: Throwable =>

        logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
          , sparkSession.conf.get("spark.app.name")
          , this.getClass.getName
          , KafkaConstants.gimelAuditRunTypeStream
          , yarnCluster
          , user
          , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
          , MethodName
          , sql
          , scala.collection.mutable.Map("sql" -> sql)
          , GimelConstants.FAILURE
          , e.toString + "\n" + e.getStackTraceString
          , GimelConstants.UNKNOWN_STRING
        )

        // throw error to console
        logger.throwError(e.toString)

        throw e
    }

  }

  private def toLogFriendlyString(str: String): String = {
    str.replaceAllLiterally("/", "_").replaceAllLiterally(" ", "-")
  }

  /**
    * handleDDLs will direct to respective data set create/drop/truncate based on the incoming DDL
    *
    * @param sql          - SQL that is passed to create/drop/delete
    * @param sparkSession - spark session
    * @param dataSet      - dataset name
    * @param options      - List of options
    * @return
    */
  def handleDDLs(sql: String, sparkSession: SparkSession, dataSet: DataSet, options: Map[String, String]): Unit = {
    val uniformSQL = sql.replace("\n", " ")
    val sqlParts: Array[String] = uniformSQL.split(" ")
    // remove all additional white spaces in the DDL statment
    val newSql = sqlParts.filter(x => !x.isEmpty).mkString(" ")
    val newSqlParts = newSql.split(" ")
    sqlParts.head.toUpperCase match {
      // We have two "create ddl" paths. One with full create (plain) statement provided by the user
      // the other where we have to construct from the dataframe after running select clause in given sql/ddl
      // create table db.tablename(x int, y varchar(10) will be handled by handlePlainCreateDDL funcation
      // create table db.tablename tblproperties("table_type":"SET") as select * from another_table.
      case QueryConstants.DDL_CREATE_STRING => {
        val index = sqlParts.indexWhere(_.toLowerCase().contains(GimelConstants.UDC_STRING))
        // Find out whether select is part of the create statement
        val isHavingSelect = QueryParserUtils.isHavingSelect(sql)
        isHavingSelect match {
          case true => handleSelectDDL(newSqlParts, newSql, dataSet, options, sparkSession)
          case false => handlePlainCreateDDL(newSqlParts, dataSet, options, sparkSession)
        }
      }
      //  following case will cover DROP DDL
      case QueryConstants.DDL_DROP_STRING => {
        val dataSetName = newSqlParts(2)
        dataSet.drop(dataSetName, options)
      }
      //  following case will cover TRUNCATE DDL
      case QueryConstants.DDL_TRUNCATE_STRING => {
        val dataSetName = newSqlParts(2)
        dataSet.truncate(dataSetName, options)
      }
      //  following case will cover both DELETE AND DELETE FROM DDL
      case QueryConstants.DDL_DELETE_STRING => {
        val dataSetName = newSqlParts.map(_.toUpperCase()).contains(QueryConstants.DDL_FROM_STRING) match {
          case true => newSqlParts(2)
          case _ => newSqlParts(1)
        }
        dataSet.truncate(dataSetName, options)
      }
      case _ => throw new Exception("Unexpected path at runtime. We should not arrive at this location !")
    }
  }

  /**
    * handleSelectDDL -
    * Strip out the the select statement
    * Run the sql using executeBatch and get the data frame back
    * Get the schema from data frame and pass it in options
    * Strip out the table properties and pass it in options
    * Create the object/table
    * Call dataSet.Write to the object/table that got created
    *
    * @param sqlParts     - each word in the sql comes as array
    * @param sql          - the full sql query
    * @param dataSet      - dataset Object itself
    * @param options      - options comings from user
    * @param sparkSession - Spark session
    * @return
    */
  def handleSelectDDL(sqlParts: Array[String], sql: String, dataSet: DataSet, options: Map[String, String], sparkSession: SparkSession): Unit = {
    val selectIndex = sqlParts.indexWhere(_.toUpperCase().contains(QueryConstants.SQL_SELECT_STRING))
    val selectClause = sqlParts.slice(selectIndex, sqlParts.length).mkString(" ")
    val pcatalogIndex = sqlParts.indexWhere(_.toLowerCase().contains(GimelConstants.UDC_STRING))
    val datasetname = sqlParts(pcatalogIndex)

    // Run the Select statement and get the results in a dataframe
    val selectDF = executeBatch(selectClause, sparkSession)
    val schema: Array[StructField] = selectDF.schema.fields

    // Check if 'PARTITIONED' clause present in the sql. If so we want to get the partitioned fileds so that we will use it during creation of the table when building CREATE TABLE statement.
    val partitionFields: Array[com.paypal.gimel.common.catalog.Field] = existsPartitionedByClause(sql) match {
      case true => getPartitionsFields(sql)
      case _ => Array[com.paypal.gimel.common.catalog.Field]()
    }

    val newOptions: Map[String, Any] = options ++ Map[String, Any](GimelConstants.TABLE_FILEDS -> schema, GimelConstants.CREATE_STATEMENT_IS_PROVIDED -> "false", GimelConstants.TABLE_SQL -> sql, GimelConstants.HIVE_DDL_PARTITIONS_STR -> partitionFields)

    // Create the table and Write data into it from the selected dataframe
    try {
        dataSet.create(datasetname, newOptions)
        logger.info("Table/object creation success")
        dataSet.write(datasetname, selectDF, newOptions)
    } catch {
      case e: Throwable =>
        val msg = s"Error creating/writing table: ${e.getMessage}"
        throw new Exception(msg, e)
    }
  }

  def handlePlainCreateDDL(sqlParts: Array[String], dataSet: DataSet, options: Map[String, String], sparkSession: SparkSession): Unit = {

    // Since select is not part of create statement it has to be full create statement
    // We need to replace the pcatalog.storagetype.storagesystem.DB.Table with DB.Table
    // So that we can pass the entire create statement as is to respective storage engines
    val index = sqlParts.indexWhere(_.toLowerCase().contains(GimelConstants.UDC_STRING))

    val datasetname = sqlParts(index)
    val newSQL = sqlParts.map(element => {
      if (element.toLowerCase().contains(GimelConstants.UDC_STRING + ".")) {
        // we replace pcatalog.storagetype.storagesystem.DB.Table with DB.Table
        element.split('.').tail.mkString(".").split('.').tail.mkString(".").split('.').tail.mkString(".")
      }
      else {
        element
      }
    }
    ).mkString(" ")
    val newOptions = options ++ Map[String, String](GimelConstants.TABLE_SQL -> newSQL.toString, GimelConstants.CREATE_STATEMENT_IS_PROVIDED -> "true")
    dataSet.create(datasetname, newOptions)

  }

  /** booltoDF will convert the boolean result to a dataframe
    *
    * @param spark  - sparksessionboolToDF
    * @param result - boolean return from the create/drop/truncate methods
    * @return
    */
  def boolToDFWithErrorString(spark: SparkSession, result: Boolean, addOnString: String): DataFrame = {
    val resultStr = if (result) "success" else "failure"
    import spark.implicits._
    result match {
      case false => throw new Exception(s"${addOnString}\n")
      case _ => Seq(resultStr).toDF("Query Execution")
    }
  }

  /** booltoDF will convert the boolean result to a dataframe
    *
    * @param spark  - sparksession
    * @param result - boolean return from the create/drop/truncate methods
    * @return
    */
  def boolToDF(spark: SparkSession, result: Boolean): DataFrame = {
    val resultStr = if (result) "success" else "failure"
    import spark.implicits._
    Seq(resultStr).toDF("Query Execution")
  }

  /** stringToDF will convert the string result to a dataframe
    *
    * @param spark  - sparksession
    * @param result - boolean return from the create/drop/truncate methods
    * @return
    */
  def stringToDF(spark: SparkSession, result: String): DataFrame = {
    import spark.implicits._
    Seq(result).toDF("Query Execution")
  }

  /**
    * From the create table SQL, parse the partitioned by clause and get all the partitions
    *
    * @param sql - Incoming sql
    * @return - Array of Fields which has partition column name with data type hard coded as String for now as it is not going to be used elsewhere
    */
  def getPartitionsFields(sql: String): Array[com.paypal.gimel.common.catalog.Field] = {
    val pattern = """^.+PARTITIONED BY \((.*?)\).+""".r
    val pattern(partitions) = sql.toUpperCase()
    var fieldsList: Array[com.paypal.gimel.common.catalog.Field] = Array[com.paypal.gimel.common.catalog.Field]()
    val listParts = partitions.split(",")
    listParts.map(parts => fieldsList :+= com.paypal.gimel.common.catalog.Field(parts, "String"))
    fieldsList
  }


  /**
    *
    * Method to check in special checks in SQL string
    *
    * @param sql
    * @return
    */
  def vulnerabilityCheck(sql: String): Unit = {

    val checkFlag = if (sql.toUpperCase.contains(s"SET ${JdbcConfigs.jdbcUserName}".toUpperCase)) {
      true
    }
    else {
      false
    }

    if (checkFlag) {
      throw new Exception(
        s"""
           |SECURITY VIOLATION | Execution of this statement is not allowed: ${sql}
        """.stripMargin)
    }
  }

}

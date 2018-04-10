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

import scala.util.{Failure, Success, Try}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.paypal.gimel._
import com.paypal.gimel.common.catalog.{CatalogProvider, DataSetProperties}
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.datastreamfactory.{StreamingResult, WrappedData}
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.logging.GimelStreamingListener

object GimelQueryProcessor extends Logger {

  lazy val pCatalogStreamingKafkaTmpTableName = "pcatalog_streaming_kafka_tmp_table"
  val queryUtils = GimelQueryUtils

  import queryUtils._

  val user = sys.env("USER")
  val yarnCluster = com.paypal.gimel.common.utilities.DataSetUtils.getYarnClusterName()

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */
  def executeBatch(sql: String, sparkSession: SparkSession): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    val options = queryUtils.getOptions(sparkSession)._2

    if (options(GimelConstants.LOG_LEVEL).toString == "CONSOLE") {
      setLogLevel("INFO")
      consolePrintEnabled = true
    }
    else setLogLevel(options(GimelConstants.LOG_LEVEL).toString)

    var resultingString = ""
    val queryTimer = Timer()
    val startTime = queryTimer.start
    val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
    val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
    debug(s"Is CheckPointing Requested By User --> $isCheckPointEnabled")
    val dataSet: DataSet = DataSet(sparkSession)
    val (originalSQL, destination, selectSQL, kafkaDataSets) = resolveSQL(sql, sparkSession, dataSet)
    destination match {
      case Some(target) =>
        info(s"Target Exists --> ${target}")
        Try(executeResolvedQuery(originalSQL, destination, selectSQL, sparkSession, dataSet)) match {
          case Success(result) =>
            resultingString = result
          case Failure(e) =>
            resultingString = s"Query Failed in function : $MethodName. Error --> \n\n ${
              e.getStackTraceString
            }"
            error(resultingString)
            throw new Exception(resultingString, e)
        }
        if (isCheckPointEnabled) kafkaDataSets.foreach(k => k.saveCheckPoint())
        if (isClearCheckPointEnabled) kafkaDataSets.foreach(k => k.clearCheckPoint())
        val json = Seq(s"""{"Query Execution":"${resultingString}"}""")
        sparkSession.read.json(sparkSession.sparkContext.parallelize(json))
      case _ =>
        info(s"No Target, returning DataFrame back to client.")
        executeSelectClause(selectSQL, sparkSession)
    }
  }

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    * Executes the executeBatch function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  def executeStream(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeStream
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    sparkSession.sparkContext.setLogLevel("ERROR")

    val defaultGimelLogLevel = sparkSession.conf.get(GimelConstants.LOG_LEVEL, "ERROR").toString
    if (defaultGimelLogLevel == "CONSOLE") {
      setLogLevel("INFO")
      consolePrintEnabled = true
    }
    else setLogLevel(defaultGimelLogLevel)

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
    debug(
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
        streamingResult.dStream.foreachRDD { (rdd, time) =>
          printStats(time, listner)
          val count = rdd.count()
          if (count > 0) {
            if (isStreamFailureBeyondThreshold) {
              if ((count / batchInterval) > streamFailureThresholdPerSecond) throw new Exception(s"Current Messages Per Second : ${count / batchInterval} exceeded Supplied Stream Capacity ${streamFailureThresholdPerSecond}")
              else info(s"Current Messages Per Second : ${count / batchInterval} within Supplied Stream Capacity ${streamFailureThresholdPerSecond}")
            }
            val failureThreshold = (batchInterval * streamFailureWindowFactor)
            val totalDelay = (listner.totalDelay / 1000)
            if (totalDelay > failureThreshold) {
              throw new Exception(
                s"""Current Total_Delay:$totalDelay exceeded $failureThreshold <MultiplicationFactor:$streamFailureWindowFactor X StreamingWindow:$batchInterval>
If mode=intelligent, then Restarting will result in Batch Mode Execution first for catchup, and automatically migrate to stream mode !
                   """.stripMargin
              )
            } else info(s"Current Total_Delay:$totalDelay within $failureThreshold <MultiplicationFactor:$streamFailureWindowFactor X StreamingWindow:$batchInterval>")
            streamingResult.getCurrentCheckPoint(rdd)
            streamingResult.getAsDF(sqlContext, rdd).registerTempTable(tmpKafkaTable)
            try {
              executeBatch(newSQL, sparkSession)
            } catch {
              case ex: Throwable =>
                error(s"Stream Query Failed in function : $MethodName. Error --> \n\n${ex.getStackTraceString}")
                ex.printStackTrace()
                error("Force - Stopping Streaming Context")
                ssc.sparkContext.stop()
                throw ex
            }
            try {
              if (isSaveCheckPointEnabled) streamingResult.saveCurrentCheckPoint()
              if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint as Requested by User")
            }
            catch {
              case ex: Throwable =>
                error("Error in CheckPoint Operations in Streaming.")
                ex.printStackTrace()
                ssc.sparkContext.stop()
            }
          }
        }
      } catch {
        case ex: Throwable =>
          error(s"ERROR In Streaming Window --> \n\n${ex.getStackTraceString}")
          ex.printStackTrace()
          ssc.sparkContext.stop()
          throw ex
      }
      dataStream.streamingContext.start()
      dataStream.streamingContext.awaitTerminationOrTimeout(streamawaitTerminationOrTimeout)
      dataStream.streamingContext.stop(false, true)
      "Success"
    }

  }

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    *
    * @return RDD[Resulting String < either sample data for select queries, or "success" / "failed" for insert queries]
    */
  def executeBatchSparkMagic: (String, SparkSession) => RDD[String] = executeBatchSparkMagicRDD

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    * Executes the executeBatchSparkMagicRDD function in streaming window
    *
    * @return RDD[Resulting String] < either sample data for select queries, or "success" / "failed" for insert queries
    */
  def executeStreamSparkMagic: (String, SparkSession) => RDD[String] = executeStreamSparkMagicRDD

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return RDD[Resulting String < either sample data for select queries, or "success" / "failed" for insert queries]
    */
  def executeBatchSparkMagicRDD(sql: String, sparkSession: SparkSession): RDD[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    val options = queryUtils.getOptions(sparkSession)._2

    if (options(GimelConstants.LOG_LEVEL).toString == "CONSOLE") {
      setLogLevel("INFO")
      consolePrintEnabled = true
    }
    else setLogLevel(options(GimelConstants.LOG_LEVEL).toString)

    var resultingRDD: RDD[String] = sparkSession.sparkContext.parallelize(Seq(""))
    val queryTimer = Timer()
    val startTime = queryTimer.start
    val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
    val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
    debug(s"Is CheckPointing Requested By User --> ${
      isCheckPointEnabled
    }")
    val dataSet: DataSet = DataSet(sparkSession)
    val (originalSQL, destination, selectSQL, kafkaDataSets) = resolveSQL(sql, sparkSession, dataSet)
    Try(executeResolvedQuerySparkMagic(originalSQL, destination, selectSQL, sparkSession, dataSet)) match {
      case Success(result) =>
        resultingRDD = result
      case Failure(e) =>
        resultingRDD = sparkSession.sparkContext.parallelize(Seq(
          s"""{"Batch Query Error" : "${
            e.getStackTraceString
          }" """))
        val resultMsg = resultingRDD.collect().mkString("\n")
        error(resultMsg)
        throw new Exception(resultMsg)
    }
    if (isCheckPointEnabled) kafkaDataSets.foreach(k => k.saveCheckPoint())
    if (isClearCheckPointEnabled) kafkaDataSets.foreach(k => k.clearCheckPoint())
    resultingRDD
  }

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    * Executes the executeBatchSparkMagicRDD function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return RDD[Resulting String] < either sample data for select queries, or "success" / "failed" for insert queries
    */

  def executeStreamSparkMagicRDD(sql: String, sparkSession: SparkSession): RDD[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    sparkSession.sparkContext.setLogLevel("ERROR")
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeStream
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    sparkSession.sparkContext.setLogLevel("ERROR")
    val options = getOptions(sparkSession)._2

    val defaultGimelLogLevel = sparkSession.conf.get(GimelConstants.LOG_LEVEL, "ERROR").toString
    if (defaultGimelLogLevel == "CONSOLE") {
      setLogLevel("INFO")
      consolePrintEnabled = true
    }
    else silence

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
        CatalogProvider.getDataSetProperties(table)
      DataSetUtils.getSystemType(dataSetProperties) == DataSetType.KAFKA
    }
    if (kafkaTables.isEmpty) {
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
                  error(s"Stream Query Failed in function : $MethodName. Error --> \n\n${ex.getStackTraceString}")
                  ex.printStackTrace()
                  error("Force - Stopping Streaming Context")
                  ssc.sparkContext.stop()
              }
              try {
                if (isSaveCheckPointEnabled) streamingResult.saveCurrentCheckPoint()
                if (isClearCheckPointEnabled) streamingResult.clearCheckPoint("Clearing CheckPoint as Requested by User")
              }
              catch {
                case ex: Throwable =>
                  error("Error in CheckPoint Operations in Streaming.")
                  ex.printStackTrace()
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
          //            dataStream.streamingContext.sparkContext.parallelize(Seq(s"""{"Error" : "${ex.getStackTraceString}" }"""))
          throw new Exception(msg)
      }
    }
  }

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    *
    * @param sql          SQL String supplied by client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  @deprecated
  def executeBatchSparkMagicJSON(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeBatch
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    val options = queryUtils.getOptions(sparkSession)._2
    setLogLevel(options(GimelConstants.LOG_LEVEL).toString)
    var resultSet = ""
    val queryTimer = Timer()
    val startTime = queryTimer.start
    val isCheckPointEnabled = options(KafkaConfigs.kafkaConsumerReadCheckpointKey).toBoolean
    val isClearCheckPointEnabled = options(KafkaConfigs.kafkaConsumerClearCheckpointKey).toBoolean
    debug(s"Is CheckPointing Requested By User --> ${
      isCheckPointEnabled
    }")
    val dataSet: DataSet = DataSet(sparkSession)
    val (originalSQL, destination, selectSQL, kafkaDataSets) = resolveSQL(sql, sparkSession, dataSet)
    Try(executeResolvedQuerySparkMagic(originalSQL, destination, selectSQL, sparkSession, dataSet)) match {
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
        error(resultSet)
        throw new Exception(resultSet)
    }
    if (isCheckPointEnabled) kafkaDataSets.foreach(k => k.saveCheckPoint())
    if (isClearCheckPointEnabled) kafkaDataSets.foreach(k => k.clearCheckPoint())
    resultSet
  }

  /**
    * Core Function that will be called from GIMEL-LOGGING for executing a SQL
    * Executes the @executeBatchSparkMagicJSON function in streaming window
    *
    * @param sql          SQL String from client
    * @param sparkSession : SparkSession
    * @return Resulting String < either sample data for select queries, or "success" / "failed" for insert queries
    */

  @deprecated
  def executeStreamSparkMagicJSON(sql: String, sparkSession: SparkSession): String = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    val sparkAppName = sparkSession.conf.get("spark.app.name")
    logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , KafkaConstants.gimelAuditRunTypeStream
      , yarnCluster
      , user
      , toLogFriendlyString(s"${yarnCluster}/${user}/${sparkAppName}")
      , MethodName
      , sql
      , scala.collection.mutable.Map("sql" -> sql)
    )
    var returnMsg = ""
    sparkSession.sparkContext.setLogLevel("ERROR")
    setLogLevel(sparkSession.conf.get(GimelConstants.LOG_LEVEL, "ERROR").toString)
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
    debug(
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
        CatalogProvider.getDataSetProperties(table)
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
                  error(returnMsg)
                  ex.printStackTrace()
                  warning("Force - Stopping Streaming Context")
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
          error(returnMsg)
          ex.printStackTrace()
          warning("Force - Stopping Streaming Context")
          ssc.sparkContext.stop()
          throw new Exception(returnMsg)
      }
      dataStream.streamingContext.start()
      dataStream.streamingContext.awaitTermination()
      dataStream.streamingContext.stop()
      returnMsg = s"""{"Stream Query" : "SUCCESS"} """
    }
    returnMsg
  }

  private def toLogFriendlyString(str: String): String = {
    str.replaceAllLiterally("/", "_").replaceAllLiterally(" ", "-")
  }
}


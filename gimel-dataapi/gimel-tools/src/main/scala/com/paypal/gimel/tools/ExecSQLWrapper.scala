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

package com.paypal.gimel.tools

import java.util.Calendar

import scala.collection.immutable.Map
import scala.language.implicitConversions

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.kafka.conf.{KafkaConfigs, KafkaConstants}
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.sql.GimelQueryProcessor
import com.paypal.gimel.tools.conf.CopyDatasetConstants

object CopyDataSet extends App {

  import CopyHelperUtils._
  import com.paypal.gimel.kafka.utilities.KafkaUtilities._

  val logger = Logger(this.getClass.getName)
  val user = sys.env("USER")
  val sparkConf = new SparkConf()
  val sparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  val props = resolveRunTimeParameters(args) ++ Map(GimelConstants.SPARK_APP_ID -> sparkSession.conf.get(GimelConstants.SPARK_APP_ID),
    GimelConstants.SPARK_APP_NAME -> sparkSession.conf.get(GimelConstants.SPARK_APP_NAME))
  props.foreach(prop => sparkSession.conf.set(prop._1, prop._2))
  logger.setSparkVersion(sparkSession.version)
  val resolvedProps = getOptions(sparkSession)
  val queryToExecute = getQuery(props)
  val sparkAppName = sparkSession.conf.get("spark.app.name")
  val yarnCluster = com.paypal.gimel.common.utilities.DataSetUtils.getYarnClusterName()
  val runMode = props("mode") match {
    case "stream" => KafkaConstants.gimelAuditRunTypeStream
    case "batch" => KafkaConstants.gimelAuditRunTypeBatch
    case "intelligent" => KafkaConstants.gimelAuditRunTypeIntelligent
    case _ => GimelConstants.UNKNOWN_STRING.toLowerCase
  }

  val hiveStagingDir = props.getOrElse("hiveStagingDir", "")
  try {
    props("mode").toLowerCase() match {
      case CopyDatasetConstants.COPY_DATASET_STREAM_MODE => GimelQueryProcessor.executeStream(queryToExecute, sparkSession)
      case CopyDatasetConstants.COPY_DATASET_BATCH_MODE => GimelQueryProcessor.executeBatch(queryToExecute, sparkSession)
      case CopyDatasetConstants.COPY_DATASET_BATCH_INTERACTIVE_MODE =>
        val isBatchInfinite = props.getOrElse("isBatchRecursionInfinite", "false").toBoolean
        val batchRecursionRequested = props.getOrElse("batchRecursionRequested", "100").toInt
        val batchRecursinMins = props.getOrElse("batchRecursionMinutes", 30).toString.toInt
        logger.info(
          s"""
             |--------------------------------------------------------------------
             || isBatchRecursionInfinite | ${isBatchInfinite}
             || batchRecursionRequested  | ${batchRecursionRequested}
             || batchRecursinMins        | ${batchRecursinMins}
             |--------------------------------------------------------------------
          """.stripMargin)
        val batchRecursionMilliSec: Double = batchRecursinMins * 60 * 1000D
        var currentIteration = 1
        while (isBatchInfinite || (currentIteration <= batchRecursionRequested)) {
          val startTime = Calendar.getInstance().getTime
          logger.info(
            s"""
               |--------------------------------------------------------------------
               || Mode        | ${props("mode")}
               || Iteration   | ${currentIteration}
               || Start Time  | ${Calendar.getInstance().getTime}
               |--------------------------------------------------------------------
             """.stripMargin)
          val timer = Timer()
          timer.start
          GimelQueryProcessor.executeBatch(queryToExecute, sparkSession)
          val totalTimeMilliSec: Double = timer.endWithMillSecRunTime
          val endTime = Calendar.getInstance().getTime
          val sleepMilliSec = scala.math.max(0, batchRecursionMilliSec - totalTimeMilliSec)
          logger.info(
            s"""
               |--------------------------------------------------------------------
               || (*)   | Iteration                     | ${currentIteration}
               || (*)   | Start Time Execution          | ${startTime}
               || (*)   | Start End Execution           | ${endTime}
               || (Y)   | Time Taken for Execution (ms) | ${totalTimeMilliSec}
               || (X)   | Batch Iteration Request (ms)  | ${batchRecursionMilliSec}
               || (X-Y) | Time Remaining for Sleep (ms) | ${sleepMilliSec}
               |--------------------------------------------------------------------
            """.stripMargin)
          if (currentIteration == batchRecursionRequested) logger.info("All Iterations Completed !")
          if (sleepMilliSec > 0 && currentIteration < batchRecursionRequested) {
            logger.info(s"Going to Sleep at --> ${Calendar.getInstance().getTime}")
            Thread.sleep(sleepMilliSec.toLong)
            logger.info(s"Woke Up at --> ${Calendar.getInstance().getTime}")
          }
          currentIteration += 1
        }
      case CopyDatasetConstants.COPY_DATASET_INTELLIGENT_MODE =>
        logger.info(s"Mode --> auto")
        var batchRunCount = 0
        while (!isStreamable(sparkSession, props)) {
          logger.info(s"====== BATCH Mode < Iteration --> ${batchRunCount} > ======")
          val timer = Timer()
          timer.start
          GimelQueryProcessor.executeBatch(queryToExecute, sparkSession)
          if (hiveStagingDir != "") sparkSession.sql(s"dfs -rm -r -f ${hiveStagingDir}")
          timer.endWithMillSecRunTime
          logger.info(s"====== BATCH Mode < Iteration --> ${batchRunCount} > Total Time Seconds --> ${timer.endWithMillSecRunTime / 1000} ====== ")
          batchRunCount = batchRunCount + 1
        }
        logger.info("====== STREAM Mode ======")
        GimelQueryProcessor.executeStream(queryToExecute, sparkSession)
      case _ => throw new Exception("Invalid Mode of Execution Must be one of these <batch|stream>")
    }

    // push logs to KAFKA
    logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
      , sparkSession.conf.get("spark.app.name")
      , this.getClass.getName
      , runMode
      , yarnCluster
      , user
      , s"${yarnCluster}/${user}/${sparkAppName}".replaceAllLiterally("/", "_").replaceAllLiterally(" ", "-")
      , "copyDataSet"
      , s"${queryToExecute}"
      , scala.collection.mutable.Map("sql" -> queryToExecute)
      , GimelConstants.SUCCESS
      , GimelConstants.EMPTY_STRING
      , GimelConstants.EMPTY_STRING
    )
  }
  catch {
    case e: Throwable => {
      e.printStackTrace()

      // push logs to KAFKA
      logger.logMethodAccess(sparkSession.sparkContext.getConf.getAppId
        , sparkSession.conf.get("spark.app.name")
        , this.getClass.getName
        , runMode
        , yarnCluster
        , user
        , s"${yarnCluster}/${user}/${sparkAppName}".replaceAllLiterally("/", "_").replaceAllLiterally(" ", "-")
        , "copyDataSet"
        , s"${queryToExecute}"
        , scala.collection.mutable.Map("sql" -> queryToExecute)
        , GimelConstants.FAILURE
        , e.toString + "\n" + e.getStackTraceString
        , GimelConstants.UNKNOWN_STRING
      )

      // throw error to console
      logger.throwError(e.toString)

      throw e
    }
  }

}

object CopyHelperUtils {

  val logger = Logger(this.getClass.getName)

  /**
   * Resolves RunTime Params
   *
   * @param allParams args
   * @return Map[String, String]
   */
  def resolveRunTimeParameters(allParams: Array[String]): Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()

    logger.info(" @Begin --> " + MethodName)

    var paramsMapBuilder: Map[String, String] = Map()
    for (jobParams <- allParams) {
      for (eachParam <- jobParams.split(" ")) {
        paramsMapBuilder += (eachParam.split("=")(0) -> eachParam.split("=", 2)(1))
      }
    }
    logger.info(s"All Params From User --> ${paramsMapBuilder.mkString("\n")}")

    val usage =
      """
        |For Details : https://github.com/Paypal/gimel/blob/oss/docs/gimel-tools/ExecSQLWrapper.md
      """.stripMargin

    if (allParams.length == 0) {
      logger.error(usage)
      throw new Exception(s"Args Cannot be Empty. Usage --> \n${usage}")
    }

    if (!paramsMapBuilder.contains("mode")) throw new Exception(s"mode must be supplied as either < batch|stream > Usage --> \n${usage}")
    if (!paramsMapBuilder.contains("querySourceFile")) throw new Exception(s"querySourceFile must be supplied ! Usage --> \n${usage}")

    logger.info(s"Resolved Params From Code --> ${paramsMapBuilder}")
    paramsMapBuilder
  }

  /**
   * getOptions - read the hive context options that was set by the user else add the default values
   *
   * @param sparkSession SparkSession
   * @return - Tuple ( String with concatenated options read from the hivecontext , Same Props as a Map[String,String] )
   */

  def getOptions(sparkSession: SparkSession): (String, Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()

    logger.info(" @Begin --> " + MethodName)

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
      , KafkaConfigs.isBackPressureEnabledKey -> "true"
      , HbaseConfigs.hbaseOperation -> "scan"
      , HbaseConfigs.hbaseFilter -> ""
      , GimelConstants.DATA_CACHE_IS_ENABLED -> "false"
      , GimelConstants.DATA_CACHE_IS_ENABLED_FOR_ALL -> "true"
    )
    val resolvedOptions: Map[String, String] = optionsToCheck.map { kvPair =>
      (kvPair._1, hiveConf.getOrElse(kvPair._1, kvPair._2))
    }
    resolvedOptions.foreach(conf => sparkSession.conf.set(conf._1, conf._2))
    (resolvedOptions.map(x => x._1 + "=" + x._2).mkString(":"), hiveConf ++ resolvedOptions)
  }

  def getQuery(props: Map[String, String]): String = {

    val sql: String = {
      logger.info(s"User Requested Execution of SQL from External File.")
      val querySourceFile = props("querySourceFile")
      val unresolvedQuery = HDFSAdminClient.readHDFSFile(querySourceFile)
      logger.info(s"SQL From External File --> \n${unresolvedQuery}")
      val replacementProps = props.filter(x => x._1.toUpperCase.startsWith("GIMEL.SQL.PARAM"))
      logger.info(
        s"""
           |Following Props will be resolved in External File's SQL String -->
           |${replacementProps.mkString("\n", "\n", "")}
          """.stripMargin)
      replacementProps.foldLeft(unresolvedQuery)((s, prop) => s.replaceAll(prop._1.toUpperCase, prop._2))
    }
    logger.info(s"Resolved Query to Execute --> ${sql}")
    sql
  }

  /**
   * getYarnClusterName - gets the yarn cluster from the hadoop config file
   *
   * @return
   */
  def getYarnClusterName(): String = {
    val hadoopConfiguration = new org.apache.hadoop.conf.Configuration()
    val cluster = hadoopConfiguration.get(GimelConstants.FS_DEFAULT_NAME)
    cluster.split("/").last
  }
}

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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.kafka.conf.KafkaConfigs
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.tools.conf.BenchmarkKafkaConstants

@deprecated
object BenchMarkKafkaDataSetAPI extends App {

  // Logger Initiation
  val logger = Logger(this.getClass.getName)

  val sparkSession = SparkSession
    .builder()
    .appName("SparkSessionZipsExample")
    .enableHiveSupport()
    .getOrCreate()
  val sc = sparkSession.sparkContext
  val sqlContext = sparkSession.sqlContext

  import BenchMarkHelperUtils._

  val paramsMapBuilder = resolveRunTimeParameters(args)
  lazy val appName = sparkSession.conf.get(GimelConstants.SPARK_APP_NAME, "NA") + "_" + sc.getConf.getAppId
  lazy val path1 = "/tmp/" + sc.sparkUser + "_" + appName + "_" + ".Data_API.DataSet.benchmark.log"
  val path = paramsMapBuilder.getOrElse("targetFile", path1)
  val fetchRowsOnFirstRun = paramsMapBuilder(BenchmarkKafkaConstants.fetchRowsKey)
  val maxRecordsPerPartition = paramsMapBuilder(BenchmarkKafkaConstants.maxRecordsPerPartitionKey)
  val minRowsPerParallel = paramsMapBuilder(BenchmarkKafkaConstants.minRowsPerPartitionKey)
  val datasetName = paramsMapBuilder("dataset")

  /**
    * START DATASET API STATS CAPTURE
    */
  val dataset = DataSet(sparkSession)
  val props = s"""${KafkaConfigs.minRowsPerParallelKey}=$minRowsPerParallel:${KafkaConfigs.rowCountOnFirstRunKey}=$fetchRowsOnFirstRun:${KafkaConfigs.maxRecordsPerPartition}=$maxRecordsPerPartition"""
  val dataDF = dataset.read(datasetName, props)

  //  val timer = Timer()
  //  timer.start;
  val timer = Timer()
  timer.start
  val myCount = dataDF.count()
  val totalMS = timer.endWithMillSecRunTime

  val executorMemoryStatus = sc.getExecutorMemoryStatus.mkString("\n")
  val totalExecutors = sc.getExecutorMemoryStatus.size
  // val executorStorageStatus = sc.getExecutorStorageStatus.map(x => "blockManagerId:" + x.blockManagerId + "|maxMem:" + x.maxMem + "|memUsed:" + x.memUsed + "|memRemaining:" + x.memRemaining).mkString("\n")

  val allConfs = sc.getConf.getAll.mkString("\n")

  /**
    * COMPOSE STATS
    */

  val toWrite =
    s"""
       |DataAPI:BenchMark Count:$myCount
       |DataAPI:totalExecutors:$totalExecutors
       |DataAPI:TotalMS:$totalMS
      """.stripMargin

  /**
    * Write Stats
    */

  logger.info(s"Writing to Path --> $path")
  HDFSAdminClient.writeHDFSFile(path, toWrite)

  sc.stop()

}

@deprecated
class CDHTimer(funcName: String) {
  val logger = Logger()

  def timed[T](f: => T): T = {
    val startTime = System.currentTimeMillis()
    try f finally println(s"Function completed in: ${System.currentTimeMillis() - startTime} ms")
  }

  var startTime: Long = -1L

  def start(): Unit = {
    startTime = System.currentTimeMillis()
  }

  def end(): Long = {
    val endTime: Long = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    logger.info("TOTAL TIME " + funcName + " elapse time = " + elapsedTime)
    elapsedTime
  }
}

@deprecated
object BenchMarkHelperUtils {

  val logger = Logger()

  /**
    * Resolves RunTime Params
    *
    * @param allParams args
    * @return Map[String, String]
    */
  def resolveRunTimeParameters(allParams: Array[String]): Map[String, String] = {

    var paramsMapBuilder: Map[String, String] = Map()
    logger.info(s"All Params From User --> ${allParams.mkString("\n")}")
    val usage =
      """
        |dataset=pcatalog.kafka_flights_log fetchRowsOnFirstRun=1000000 maxRecordsPerPartition=1000000 targetFile=/tmp/stats/log"
      """.stripMargin
    if (allParams.length == 0) {
      println(usage)
      throw new Exception("Args Cannot be Empty")
    }
    for (jobParams <- allParams) {
      for (eachParam <- jobParams.split(" ")) {
        paramsMapBuilder += (eachParam.split("=")(0) -> eachParam.split("=", 2)(1))
      }
    }
    if (!paramsMapBuilder.contains("dataset")) paramsMapBuilder += ("dataset" -> "pcatalog.kafka_flights_log")
    if (!paramsMapBuilder.contains(BenchmarkKafkaConstants.fetchRowsKey)) paramsMapBuilder += (BenchmarkKafkaConstants.fetchRowsKey -> "1000000")
    if (!paramsMapBuilder.contains(BenchmarkKafkaConstants.maxRecordsPerPartitionKey)) paramsMapBuilder += (BenchmarkKafkaConstants.maxRecordsPerPartitionKey -> "1000000")
    if (!paramsMapBuilder.contains(BenchmarkKafkaConstants.minRowsPerPartitionKey)) paramsMapBuilder += (BenchmarkKafkaConstants.minRowsPerPartitionKey -> "100000")
    logger.info(s"Resolved Params From Code --> $paramsMapBuilder")
    paramsMapBuilder
  }
}

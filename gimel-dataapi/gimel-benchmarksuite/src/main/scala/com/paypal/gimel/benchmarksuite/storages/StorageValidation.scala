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

package com.paypal.gimel.benchmarksuite.storages

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.logger.Logger

abstract class StorageValidation(val dataset: DataSet, val sparkSession: SparkSession, val pcatProps: GimelBenchmarkProperties, val testData: DataFrame) extends Logger {

  /**
    * Holder for All Stats Collected in the Validation
    */
  var stats: Map[String, String] = Map()

  /**
    * Holder for All DDLs Collected in the Validation, though the Stats Map can be used for same purpose - this additional object provides some distinction
    */
  var ddls: Map[String, String] = Map()


  /**
    * Assign the DataSetName and Name using NativeAPI while implementing
    */
  val dataSetName: String
  val nativeName: String

  /**
    * Extend & Implement all the bootStrap Functionalities
    *
    * @return A Tuple of (DDL , STATS)
    */
  def bootStrap(): (Map[String, String], Map[String, String]) = (ddls, stats)

  /**
    * Extend & Implement all the cleanUp Functionalities
    *
    * @return A Tuple of (DDL , STATS)
    */
  def cleanUp(): (Map[String, String], Map[String, String]) = (ddls, stats)


  /**
    * Extend & Implement all the Benchmark Functionalities
    *
    * @return A Tuple of (DDL , STATS)
    */

  def benchmark(): (Map[String, String], Map[String, String]) = (ddls, stats)

  /**
    * Main Place to implement Validation Steps
    *
    * @param testDataOption DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  def benchmarkDatasetAPI(testDataOption: Option[DataFrame] = None, storageType: String): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"
    val datasetReadApiKey = s"$tag-" + "Read"
    val datasetWriteApiKey = s"$tag-" + "Write"
    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Dataset")

    try {
      // val testData = prepareBenchMarkTestData(pcatProps.benchMarkTestSampleRowsCount.toInt)
      val testData = testDataOption.get
      val testDataCount = testData.count()
      info(s"$tag | TestDataCount_Dataset $testDataCount")
      val dataSet = dataSetName
      info(s"$tag | Begin Write to $dataSet...")
      val datasetWriteTimer = Timer()
      datasetWriteTimer.start
      dataset.write(dataSet, testData)
      val datasetWriteTimeValue = datasetWriteTimer.endWithMillSecRunTime / 1000
      stats += (s"$datasetWriteApiKey" -> s"$datasetWriteTimeValue")
      resultsAPIData += (s"writeTime" -> s"$datasetWriteTimeValue")
      info(s"$tag | Write Success.")
      info(s"$tag | Read from $dataSet...")
      val datasetReadTimer = Timer()
      datasetReadTimer.start
      val readDF = dataset.read(dataSet)
      val count = readDF.count()
      val datasetReadTimeValue = datasetReadTimer.endWithMillSecRunTime / 1000
      stats += (s"$datasetReadApiKey" -> s"$datasetReadTimeValue")
      resultsAPIData += (s"readTime" -> s"$datasetReadTimeValue")
      info(s"$tag | Read Count $count...")
      info(s"$tag | Sample 10 Rows -->")
      readDF.cache()
      readDF.show(10)
      compareDataFrames(testData, readDF)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
      resultsAPIData += (s"StorageType" -> s"$storageType")
      bootStrapESIndexForStats()
      postResults(resultsAPIData)
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats)
  }


  /**
    * execute will be the Main Function Called from the Test Suit
    *
    * @return A Tuple of (DDL , STATS)
    */
  def execute(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    // cleanUp()
    bootStrap()
    benchmark()
    cleanUp()
    (ddls, stats)
  }


  /**
    * One Place to Handle Exception
    *
    * @param ex      Throwable Exception
    * @param message A Custom Message
    */
  def handleException(ex: Throwable, message: String = ""): Nothing = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    if ((message contains "cleanUpES") || (message contains "cleanUpESHive") || (message contains "cleanUpHBase") || (message contains "cleanUpHive")) {
      // Do nothing
    } else {
      cleanUp()
    }
    ex.printStackTrace()
    throw new Exception(s"An Error Occurred <$message>")
  }

  /**
    * Compares Two DataFrames & Fails if the data dont Match
    *
    * @param left  DataFrame
    * @param right DataFrame
    * @param tag   Some Identifier from the Caller
    */
  def compareDataFrames(left: DataFrame, right: DataFrame, tag: String = "Not Passed"): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    val testDataColumns = left.columns
    val writeMinusRead = left.selectExpr(testDataColumns: _*).except(right.selectExpr(testDataColumns: _*))
    val readMinusWrite = right.selectExpr(testDataColumns: _*).except(left.selectExpr(testDataColumns: _*))
    val writeMinusReadCount = writeMinusRead.count()
    val readMinusWriteCount = readMinusWrite.count()
    val diffCount = writeMinusReadCount + readMinusWriteCount
    val diffMsg =
      s"""
         |writeMinusReadCount --> $writeMinusReadCount
         |readMinusWriteCount --> $readMinusWriteCount
         |""".stripMargin
    info(s"Differences --> $diffMsg")
    if (diffCount != 0) {
      val ex = new Exception(diffMsg)
      info("writeMinusRead --> \n")
      writeMinusRead.show
      info("readMinusWrite --> \n")
      readMinusWrite.show
      handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
  }


  /**
    * Bootsrap ES for Stats -Both Native and Dataset
    *
    *
    */
  def bootStrapESIndexForStats(): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    try {
      val esDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS ${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestResultEsIndex}
           |(
           |  `cluster` string,
           |  `StorageType` string,
           |  `executorMemoryStatus` string,
           |  `executorStorageStatus` string,
           |  `readTime` bigint,
           |  `runId` bigint,
           |  `runTag` bigint,
           |  `runTime` bigint,
           |  `sparkDriverCores` bigint,
           |  `sparkDriverMemory` bigint,
           |  `sparkExecutorCores` bigint,
           |  `sparkExecutorMemory` bigint,
           |  `sparkExecutorsInstances` bigint,
           |  `totalDriversAndExecutors` bigint,
           |  `type` string,
           |  `user` string,
           |  `volume` bigint,
           |  `writeTime` bigint,
           |  `runTimestamp` String
           |)
           |ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
           |STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
           |WITH SERDEPROPERTIES ('serialization.format'='1')
           |LOCATION
           |  '${pcatProps.benchMarkTestHiveLocation}/${pcatProps.benchMarkTestResultEsIndex}'
           |TBLPROPERTIES (
           |  '${ElasticSearchConfigs.esIndexAutoCreate}'='true',
           |  '${GimelConstants.ES_NODE}'='${pcatProps.benchMarkTestResultEsHost}',
           |  '${GimelConstants.ES_PORT}'='${pcatProps.benchMarkTestResultEsPort}',
           |  '${ElasticSearchConfigs.esResource}'='${pcatProps.benchMarkTestResultEsIndex}/data',
           |  '${ElasticSearchConfigs.esMappingId}' = 'runTime',
           |  '${ElasticSearchConfigs.esMappingNames}' ='cluster:cluster,
           |  StorageType:StorageType,
           |  executorMemoryStatus:executorMemoryStatus,
           |  executorStorageStatus:executorStorageStatus,
           |  readTime:readTime,
           |  runId:runId,
           |  runTag:runTag,
           |  runTime:runTime,
           |  sparkDriverCores:sparkDriverCores,
           |  sparkDriverMemory:sparkDriverMemory,
           |  sparkExecutorCores:sparkExecutorCores,
           |  sparkExecutorMemory:sparkExecutorMemory,
           |  sparkExecutorsInstances:sparkExecutorsInstances,
           |  totalDriversAndExecutors:totalDriversAndExecutors,
           |  type:type,
           |  user:user,
           |  volume:volume,
           |  writeTime:writeTime,
           |  runTimestamp:@timestamp'
           |)
      """.stripMargin

      sparkSession.sql(esDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("SmokeTestResultHiveTableDDL" -> esDDL)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Takes a Map of Stats, Constructs additional information on current run, Posts to ES
    *
    * @param stats Map[Key, Value]
    */
  def postResults(stats: Map[String, String]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)
    val cluster = s"${pcatProps.cluster}".toString
    val sessionName = sparkSession.sparkContext.appName
    val sessionUser = sparkSession.sparkContext.sparkUser
    val currTime = Calendar.getInstance().getTimeInMillis
    // val sparkExecutorsStatus = sparkSession.sparkContext.getExecutorMemoryStatus
    val sparkExecutorMemory: String = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_EXECUTOR_MEMORY)
    val sparkDriverMemory = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_DRIVER_MEMORY)
    val sparkExecutorsInstances = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_EXECUTOR_INSTANCES)
    val sparkDriverCores = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_DRIVER_CORES)
    val sparkExecutorCores = sparkSession.sparkContext.getConf.get(GimelConstants.SPARK_EXECUTOR_CORES)
    val totalExecutors = sparkSession.sparkContext.getExecutorMemoryStatus.size.toString
    val executorMemoryStatus = sparkSession.sparkContext.getExecutorMemoryStatus.mkString("\n")
    val executorStorageStatus = sparkSession.sparkContext.getExecutorStorageStatus.map(x => "blockManagerId:" + x.blockManagerId + "|maxMem:" + x.maxMem + "|memUsed:" + x.memUsed + "|memRemaining:" + x.memRemaining).mkString("\n")

    val statsKeys = Map(
      "runTime" -> currTime.toString
      , "cluster" -> cluster.toString
      , "user" -> sessionUser
      , "runTag" -> sessionName
      , "runId" -> s"${pcatProps.tagId}"
      , "runTimestamp" -> currTime.toString
      , "volume" -> s"${pcatProps.benchMarkTestSampleRowsCount}"
      , "sparkExecutorMemory" -> sparkExecutorMemory
      , "sparkDriverMemory" -> sparkDriverMemory
      , "sparkExecutorsInstances" -> sparkExecutorsInstances
      , "sparkDriverCores" -> sparkDriverCores
      , "sparkExecutorCores" -> sparkExecutorCores
      , "totalDriversAndExecutors" -> totalExecutors
      , "executorMemoryStatus" -> executorMemoryStatus
      , "executorStorageStatus" -> executorStorageStatus
    )
    val esOptionsMapping = Map(ElasticSearchConfigs.esMappingId -> "runTime")
    val toPostStats: Map[String, String] = statsKeys ++ stats
    val jsonStats: String = toPostStats.toJson.compactPrint
    // val jsonStats: String = toPostStats.toJson.toString()
    val postableStats: RDD[String] = sparkSession.sparkContext.parallelize(Seq(jsonStats))
    val statsDF: DataFrame = sparkSession.read.json(postableStats)
    dataset.write(s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestResultEsIndex}", statsDF, esOptionsMapping)
    info(s"Stats --> $jsonStats")
    info(s"Posted Stats to --> ${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestResultEsIndex}")
  }


}

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

import org.apache.spark.sql._
import org.elasticsearch.spark.sql._

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs

class ElasticSearchValidation(dataset: DataSet, sparkSession: SparkSession, sqlContext: SQLContext, pcatProps: GimelBenchmarkProperties, testData: DataFrame)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, pcatProps: GimelBenchmarkProperties, testData: DataFrame) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val nativeName = s"${pcatProps.benchMarkTestESNativeIndex}/data"
  val dataSetName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestESDatasetIndex}"


  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    cleanUpESHive()
    val url_dataset = s"${pcatProps.benchMarkTestResultEsHost}:${pcatProps.benchMarkTestResultEsPort}/${pcatProps.benchMarkTestESDatasetIndex}"
    val url_native = s"${pcatProps.benchMarkTestResultEsHost}:${pcatProps.benchMarkTestResultEsPort}/${pcatProps.benchMarkTestESNativeIndex}"
    cleanUpES(url_dataset)
    cleanUpES(url_native)

  }

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapESHive()
  }


  /**
    * Benchmark ES
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def benchmark(): (Map[String, String], Map[String, String]) = {
    benchmarkES()
  }


  /**
    * Benchmark ES -Both Native and Dataset
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def benchmarkES() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"

    try {
      val testDataCount = testData.count()
      val testDataOption = Some(testData)
      val dataSet = dataSetName
      val nativeAPISet = nativeName
      logger.info(s"${tag} | TestDataCount ${testDataCount}")
      logger.info(s"${tag} | Begin Bench Mark Test..")
      logger.info(s"${tag} | Begin Bench Mark Native API to ${nativeAPISet}...")
      benchmarkNativeESAPI(testDataOption)
      logger.info(s"${tag} | End Bench Mark Native API to ${nativeAPISet}...")
      logger.info(s"${tag} | Begin Bench Mark Dataset API to ${dataSet}...")
      benchmarkDatasetAPI(testDataOption, "ES")
      logger.info(s"${tag} | End Bench Mark Dataset API to ${dataSet}...")
    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }


  /**
    * CleanUp ES Hive Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpESHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropDDL = s"drop table if exists ${dataSetName}"
      sparkSession.sql(dropDDL)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("ES_Drop_Table" -> dropDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Delete ES Index
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpES(url: String) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    logger.info("delete index")
    logger.info(url)
    try {
      val output = storageadmin.ESAdminClient.deleteIndex(url)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("ES_Index Dropped_Status" -> output)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }


  /**
    * Creates ES Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapESHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val esDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS ${dataSetName}
           |(
           |  `data` string COMMENT 'from deserializer'
           |)
           |ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
           |STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
           |WITH SERDEPROPERTIES ('serialization.format'='1')
           |LOCATION
           |  '${pcatProps.benchMarkTestHiveLocation}/${pcatProps.benchMarkTestESDatasetIndex}'
           |TBLPROPERTIES (
           |  '${ElasticSearchConfigs.esIndexAutoCreate}'='true',
           |  '${GimelConstants.ES_NODE}'='${pcatProps.esHost}',
           |  '${GimelConstants.ES_PORT}'='${pcatProps.esPort}',
           |  '${ElasticSearchConfigs.esResource}'='${pcatProps.benchMarkTestESDatasetIndex}/data'
           |)
      """.stripMargin

      sparkSession.sql(esDDL)

      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_es" -> esDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Benchmark Native ES and post stats to ES
    *
    * @return A Tuple of (DDL , STATS)
    */
  def benchmarkNativeESAPI(testDataOption: Option[DataFrame] = None): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"
    val NativeReadApiKey = s"${tag}-" + "Read"
    val NativeWriteApiKey = s"${tag}-" + "Write"
    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Native")

    try {
      val options: Map[String, String] = Map("es.resource" -> nativeName, "es.nodes" -> s"${pcatProps.benchMarkTestResultEsHost}", "es.port" -> s"${pcatProps.benchMarkTestResultEsPort}", "es.index.auto.create" -> "true")
      val testData = testDataOption.get
      val testDataCount = testData.count()
      logger.info(s"${tag} | TestDataCount_Native ${testDataCount}")
      val dataSet = nativeName
      logger.info(s"${tag} | Begin Write to ${dataSet}...")
      val nativewriteTimer = Timer()
      nativewriteTimer.start
      testData.saveToEs(nativeName, options)
      val nativeWriteTimeValue = nativewriteTimer.endWithMillSecRunTime / 1000
      stats += (s"${NativeWriteApiKey}" -> s"${nativeWriteTimeValue}")
      resultsAPIData += (s"writeTime" -> s"${nativeWriteTimeValue}")
      logger.info(s"${tag} | Write Native API Success.")
      logger.info(s"${tag} | Read from Native API ${dataSet}...")
      val nativereadTimer = Timer()
      nativereadTimer.start
      val nativeDF = sqlContext.esDF(nativeName, options)
      val count = nativeDF.count()
      val nativeReadTimeValue = nativereadTimer.endWithMillSecRunTime / 1000
      stats += (s"${NativeReadApiKey}" -> s"${nativeReadTimeValue}")
      resultsAPIData += (s"readTime" -> s"${nativeReadTimeValue}")
      logger.info(s"${tag} | Read Count ${count}...")
      logger.info(s"${tag} | Sample 10 Rows -->")
      nativeDF.cache()
      nativeDF.show(10)
      compareDataFrames(testData, nativeDF)
      stats += (s"${tag}" -> s"Success @ ${Calendar.getInstance.getTime}")
      resultsAPIData += (s"StorageType" -> s"ES")
      bootStrapESIndexForStats()
      postResults(resultsAPIData)

    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }


}

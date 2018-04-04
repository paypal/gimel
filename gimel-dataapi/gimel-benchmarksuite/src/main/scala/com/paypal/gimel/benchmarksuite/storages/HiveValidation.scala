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

import com.paypal.gimel.DataSet
import com.paypal.gimel.benchmarksuite.utilities.GimelBenchmarkProperties
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.hive.utilities.HiveSchemaUtils

class HiveValidation(dataset: DataSet, sparkSession: SparkSession, sqlContext: SQLContext, pcatProps: GimelBenchmarkProperties, testData: DataFrame)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, pcatProps: GimelBenchmarkProperties, testData: DataFrame) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val nativeName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHiveTable_NativeAPI}"
  val dataSetName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHiveTable_DatasetAPI}"


  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    cleanUpHive()
  }

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapHive
  }

  /**
    * Benchmark Hive
    *
    * @return A Tuple of (DDL , STATS)
    */

  override def benchmark(): (Map[String, String], Map[String, String]) = {
    benchmarkHive
  }


  /**
    * Benchmark Hive -Both Native and ES
    *
    * @return A Tuple of (DDL , STATS)
    */

  private def benchmarkHive = {
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
      benchmarkNativeHiveAPI(testDataOption)
      logger.info(s"${tag} | End Bench Mark Native API to ${nativeAPISet}...")
      logger.info(s"${tag} | Begin Bench Mark Dataset API to ${dataSet}...")
      benchmarkDatasetAPI(testDataOption, "Hive")
      logger.info(s"${tag} | End Bench Mark Dataset API to ${dataSet}...")
    } catch {
      case ex: Throwable =>
        stats += (s"${tag}" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
    (ddls, stats)
  }


  /**
    * Drops the Regular Hive Table used for testing DataSet.Read API
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropTableDDL_Native = s"drop table if exists ${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHiveTable_NativeAPI}"
      val deleteHDFSStmt_Native = s"dfs -rm -r -f /tmp/${pcatProps.benchMarkTestHiveTable_NativeAPI}"
      sparkSession.sql(dropTableDDL_Native)
      sparkSession.sql(deleteHDFSStmt_Native)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Hive_Table_Drop_DDL_Native" -> dropTableDDL_Native)
      ddls += ("Hive_Table_Delete_HDFS_Native" -> deleteHDFSStmt_Native)
      val dropTableDDL_Dataset = s"drop table if exists ${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHiveTable_DatasetAPI}"
      val deleteHDFSStmt_Dataset = s"dfs -rm -r -f /tmp/${pcatProps.benchMarkTestHiveTable_DatasetAPI}"
      sparkSession.sql(dropTableDDL_Dataset)
      sparkSession.sql(deleteHDFSStmt_Dataset)
      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Hive_Table_Drop_DDL" -> dropTableDDL_Dataset)
      ddls += ("Hive_Table_Delete_HDFS" -> deleteHDFSStmt_Dataset)


      // nativeAPIData += ("Hive_Table_Drop_DDL_Native" -> dropTableDDL_Native)
      // nativeAPIData += ("Hive_Table_Delete_HDFS_Native" -> deleteHDFSStmt_Native)
      // datasetAPIData += ("Hive_Table_Drop_DDL" -> dropTableDDL_Dataset)
      // datasetAPIData += ("Hive_Table_Delete_HDFS" -> deleteHDFSStmt_Dataset)


      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Creates Regular Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val sampleDF = testData.limit(10)
      val columnSet: Array[String] = sampleDF.columns
      val hiveDDL_Native = HiveSchemaUtils.generateTableDDL("external"
        , pcatProps.benchMarkTestHiveDB
        , pcatProps.benchMarkTestHiveTable_NativeAPI
        , s"${pcatProps.benchMarkTestHiveLocation}/${pcatProps.benchMarkTestHiveTable_NativeAPI}"
        , s"${pcatProps.benchmarkTestHiveFormat}"
        , columnSet)
      sparkSession.sql(hiveDDL_Native)

      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hive_Native" -> hiveDDL_Native)
      val hiveDDL_Datset = HiveSchemaUtils.generateTableDDL("external"
        , pcatProps.benchMarkTestHiveDB
        , pcatProps.benchMarkTestHiveTable_DatasetAPI
        , s"${pcatProps.benchMarkTestHiveLocation}/${pcatProps.benchMarkTestHiveTable_DatasetAPI}"
        , s"${pcatProps.benchmarkTestHiveFormat}"
        , columnSet)
      sparkSession.sql(hiveDDL_Datset)

      stats += (s"${MethodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hive_Dataset" -> hiveDDL_Datset)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${MethodName}")
    }
  }

  /**
    * Benchmark Native Hive
    *
    * @return A Tuple of (DDL , STATS)
    */

  def benchmarkNativeHiveAPI(testDataOption: Option[DataFrame] = None): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${MethodName}-${storage}"
    val NativeReadApiKey = s"${tag}-" + "Read"
    val NativeWriteApiKey = s"${tag}-" + "Write"
    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Native")

    try {
      val testData = testDataOption.get
      val testDataCount = testData.count()
      logger.info(s"${tag} | TestDataCount_Native ${testDataCount}")
      val dataSet = nativeName
      logger.info(s"${tag} | Begin Write to ${dataSet}...")
      val nativewriteTimer = Timer()
      nativewriteTimer.start
      testData.write.mode("overwrite").saveAsTable(dataSet)
      val nativeWriteTimeValue = nativewriteTimer.endWithMillSecRunTime / 1000
      stats += (s"${NativeWriteApiKey}" -> s"${nativeWriteTimeValue}")
      resultsAPIData += (s"writeTime" -> s"${nativeWriteTimeValue}")
      logger.info(s"${tag} | Write Native API Success.")
      logger.info(s"${tag} | Read from Native API ${dataSet}...")
      val nativereadTimer = Timer()
      nativereadTimer.start
      val nativeDF = sparkSession.read.table(dataSet)
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
      resultsAPIData += (s"StorageType" -> s"Hive")
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

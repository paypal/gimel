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
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.HBaseAdminClient
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.hive.utilities.HiveSchemaUtils

class HBaseValidation(dataset: DataSet, sparkSession: SparkSession, sqlContext: SQLContext, pcatProps: GimelBenchmarkProperties, testData: DataFrame)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, pcatProps: GimelBenchmarkProperties, testData: DataFrame) {

  info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHBASEHiveTable_Dataset}"
  val nativeName = s"${pcatProps.benchMarkTestHiveDB}.${pcatProps.benchMarkTestHBASEHiveTable_Native}"

  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    cleanUpHBase()
  }

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapHBase()
    bootStrapHBaseHive()
  }

  /**
    * Benchmark HBase
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def benchmark(): (Map[String, String], Map[String, String]) = {
    benchmarkHBase()
  }

  /**
    * Benchmark Hbase -Both Native and Dataset
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def benchmarkHBase() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"

    try {
      val testDataCount = testData.count()
      val testDataOption = Some(testData)
      val dataSet = dataSetName
      val nativeAPISet = nativeName
      info(s"$tag | TestDataCount $testDataCount")
      info(s"$tag | Begin Bench Mark Test..")
      info(s"$tag | Begin Bench Mark Native API to $nativeAPISet...")
      benchmarkNativeHbaseAPI(testDataOption)
      info(s"$tag | End Bench Mark Native API to $nativeAPISet...")
      info(s"$tag | Begin Bench Mark Dataset API to $dataSet...")
      benchmarkDatasetAPI(testDataOption, "Hbase")
      info(s"$tag | End Bench Mark Dataset API to $dataSet...")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats)
  }


  /**
    * Creates Table in HBASE Cluster
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHBase() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    try {
      cleanUpHBase()
      HBaseAdminClient.createHbaseTableIfNotExists(
        pcatProps.hbaseNameSpace
        , pcatProps.benchMarkTestHBASETable_Dataset
        , pcatProps.benchMarkTestHBASETableColumnFamily
        , pcatProps.benchMarkTestHBASESiteXMLHDFS
      )
      HBaseAdminClient.createHbaseTableIfNotExists(
        pcatProps.hbaseNameSpace
        , pcatProps.benchMarkTestHBASETable_Native
        , pcatProps.benchMarkTestHBASETableColumnFamily
        , pcatProps.benchMarkTestHBASESiteXMLHDFS
      )
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Creates HBASE Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHBaseHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    try {
      val hbaseNameSpace: String = pcatProps.hbaseNameSpace
      val hbaseTableName_Dataset: String = pcatProps.benchMarkTestHBASETable_Dataset
      val hbaseTableName_Native: String = pcatProps.benchMarkTestHBASETable_Native
      val hbaseHiveTableName_Dataset: String = pcatProps.benchMarkTestHBASEHiveTable_Dataset
      val hbaseHiveTableName_Native: String = pcatProps.benchMarkTestHBASEHiveTable_Native
      val hiveDB: String = pcatProps.benchMarkTestHiveDB
      val hiveLocation: String = pcatProps.benchMarkTestHiveLocation
      val sampleDF = testData.limit(10)
      val columnSet: Array[String] = sampleDF.columns
      val rowkeyColumn: String = pcatProps.benchMarkTestHBASETableRowKey
      val columnFamily: String = pcatProps.benchMarkTestHBASETableColumnFamily
      val hbaseDDL_Dataset = HiveSchemaUtils.generateTableDDL(hiveDB, hiveLocation, hbaseNameSpace, hbaseTableName_Dataset, columnSet, hbaseHiveTableName_Dataset, rowkeyColumn, columnFamily).replaceAllLiterally(";", "")
      val hbaseDDL_Native = HiveSchemaUtils.generateTableDDL(hiveDB, hiveLocation, hbaseNameSpace, hbaseTableName_Native, columnSet, hbaseHiveTableName_Native, rowkeyColumn, columnFamily).replaceAllLiterally(";", "")
      print("HIVEHABSEDDL_NATIVE")
      println(hbaseDDL_Native).toString
      sparkSession.sql(hbaseDDL_Dataset)
      sparkSession.sql(hbaseDDL_Native)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hbase_Dataset" -> hbaseDDL_Dataset)
      ddls += ("DDL_hbase_Native" -> hbaseDDL_Native)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Drops the HBASE Table in HBASE cluster
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpHBase() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    try {
      info("Dropping HBASE table --> ")
      storageadmin.HBaseAdminClient.deleteHbaseTable(
        pcatProps.hbaseNameSpace
        , pcatProps.benchMarkTestHBASETable_Dataset
        , pcatProps.benchMarkTestHBASESiteXMLHDFS
      )
      storageadmin.HBaseAdminClient.deleteHbaseTable(
        pcatProps.hbaseNameSpace
        , pcatProps.benchMarkTestHBASETable_Native
        , pcatProps.benchMarkTestHBASESiteXMLHDFS
      )
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Benchmark Native Hbase
    *
    * @return A Tuple of (DDL , STATS)
    */

  def benchmarkNativeHbaseAPI(testDataOption: Option[DataFrame] = None): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"
    val datasetReadApiKey = s"$tag-" + "Read"
    val datasetWriteApiKey = s"$tag-" + "Write"
    var resultsAPIData: Map[String, String] = Map()
    resultsAPIData += (s"type" -> s"Native")

    try {
      // val testData = prepareBenchMarkTestData(pcatProps.benchMarkTestSampleRowsCount.toInt)
      val testData = testDataOption.get
      val testDataCount = testData.count()
      info(s"$tag | TestDataCount_Dataset $testDataCount")
      val dataSet = nativeName
      info(s"$tag | Begin Write to $dataSet...")
      val datasetWriteTimer = Timer()
      val options_write: Map[String, Any] = Map("operation" -> "put")
      datasetWriteTimer.start
      dataset.write(dataSet, testData, options_write)
      val datasetWriteTimeValue = datasetWriteTimer.endWithMillSecRunTime / 1000
      stats += (s"$datasetWriteApiKey" -> s"$datasetWriteTimeValue")
      resultsAPIData += (s"writeTime" -> s"$datasetWriteTimeValue")
      info(s"$tag | Write Success.")

      info(s"$tag | Read from $dataSet...")
      val options_read: Map[String, Any] = Map("useHive" -> true, "hbase.namespace.name" -> "adp_bdpe")
      val datasetReadTimer = Timer()
      datasetReadTimer.start
      val readDF = dataset.read(dataSet, options_read).cache
      val count = readDF.count()
      val datasetReadTimeValue = datasetReadTimer.endWithMillSecRunTime / 1000
      stats += (s"$datasetReadApiKey" -> s"$datasetReadTimeValue")
      resultsAPIData += (s"readTime" -> s"$datasetReadTimeValue")
      info(s"$tag | Read Count $count...")
      info(s"$tag | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testData, readDF)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")

      // resultsAPIData += (s"readTime" -> s"0")
      resultsAPIData += (s"StorageType" -> s"Hbase")
      bootStrapESIndexForStats()
      postResults(resultsAPIData)
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats)
  }


}

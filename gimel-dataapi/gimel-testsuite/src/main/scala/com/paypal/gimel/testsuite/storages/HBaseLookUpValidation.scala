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

package com.paypal.gimel.testsuite.storages

import java.util.Calendar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.HBaseAdminClient
import com.paypal.gimel.hbase.conf.HbaseConfigs
import com.paypal.gimel.hive.utilities.HiveSchemaUtils
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class HBaseLookUpValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

  info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestHBASEHiveTable}"

  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = withMethdNameLogging { methodName =>
    cleanUpHBase()
    cleanUpHBaseHive()
    (ddls, stats)
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
    * Creates Table in HBASE Cluster
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHBase() = withMethdNameLogging { methodName =>
    try {
      // cleanUpHBase()
      HBaseAdminClient.createHbaseTableIfNotExists(
        gimelProps.hbaseNameSpace
        , gimelProps.smokeTestHBASETable
        , gimelProps.smokeTestHBASETableColumnFamily.split(",")
        , gimelProps.smokeTestHBASESiteXMLHDFS
      )
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Prepare Dummy/Sample Data for SmokeTest
    *
    * @param numberOfRows Total Number of Sample Rows to Prep
    * @return DataFrame
    */
  override def prepareSmokeTestData(numberOfRows: Int = 1000): DataFrame = withMethdNameLogging { methodName =>
    try {
      def stringed(n: Int) = s"""{"id": $n,"name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""

      val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
      val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
      val dataFrameToWrite: DataFrame = sparkSession.read.json(rdd)

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      dataFrameToWrite
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Creates HBASE Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHBaseHive() = withMethdNameLogging { methodName =>
    try {
      val hbaseNameSpace: String = gimelProps.hbaseNameSpace
      val hbaseTableName: String = gimelProps.smokeTestHBASETable
      val hbaseHiveTableName: String = gimelProps.smokeTestHBASEHiveTable
      val hiveDB: String = gimelProps.smokeTestHiveDB
      val columnSet: Array[String] = gimelProps.smokeTestHBASETableColumns.split('|').flatMap(x => x.split(':')(1).split(','))
      val rowkeyColumn: String = gimelProps.smokeTestHBASETableRowKey
      val cfColsMap: Map[String, Array[String]] = gimelProps.smokeTestHBASETableColumns.split('|').map(x => (x.split(':')(0), x.split(':')(1).split(','))).toMap
      val hbaseDDL = HiveSchemaUtils.generateTableDDL(hiveDB, hbaseHiveTableName, hbaseNameSpace, hbaseTableName, columnSet, rowkeyColumn, cfColsMap).replaceAllLiterally(";", "")
      info("Hbase Hive DDL: " + hbaseDDL)
      // we are adding these jars because Hive Session needs these jar for executing the above DDL(It needs hbase-hadoop jar for Hbase Handler)
      // we are not using hiveContext.sql because spark 2.1 version doesnt support Stored by Hbase Storage Handler.so we are replacing with Hive JDBC as it supports both versions(1.6 and 2.1)

      deployDDL(hbaseDDL)

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hbase" -> hbaseDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Main Place to implement Validation Steps
    *
    * @param testData DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  override def validateAPI(testData: Option[DataFrame] = None): (Map[String, String], Map[String, String], Option[DataFrame]) = withMethdNameLogging { methodName =>
    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"${methodName}-$storage"
    try {
      val testData = prepareSmokeTestData(gimelProps.smokeTestSampleRowsCount.toInt)
      val dataSet = dataSetName
      info(s"$tag | Begin Write to $dataSet...")
      dataset.write(dataSet, testData)
      info(s"$tag | Write Success.")

      val rowKeyColumn: String = gimelProps.smokeTestHBASETableRowKey

      // Validating LookUp on rowKey
      info(s"$tag | Read from $dataSet... - Lookup on rowKey = 1")
      val testDataRowKey = testData.filter(col(rowKeyColumn) === "1").drop(rowKeyColumn)
      val optionsRowKey: Map[String, Any] = Map(HbaseConfigs.hbaseOperation -> "get", HbaseConfigs.hbaseFilter -> "rowKey=1")
      val readDFRowKey = dataset.read(dataSet, optionsRowKey)
      val countRowKey = readDFRowKey.cache.count()
      info(s"$tag | Read Count $countRowKey...")
      info(s"$tag | Sample 10 Rows -->")
      readDFRowKey.show(10)
      compareDataFrames(testDataRowKey, readDFRowKey)

      // Validating LookUp on rowKey+ColumnFamily
      val cfColsMap: Map[String, Array[String]] = gimelProps.smokeTestHBASETableColumns.split('|').map(x => (x.split(':')(0), x.split(':')(1).split(','))).toMap
      val cfToGet = gimelProps.smokeTestHBASETableColumnFamily.split(",")(0)
      info(s"$tag | Read from $dataSet... - Lookup on rowKey = 1 and column family = " + cfToGet)
      val columnsToInclude: Array[String] = cfColsMap.getOrElse(cfToGet, Array.empty[String])
      val testDataRowKeyCf = testData.filter(col(rowKeyColumn) === "1").select(columnsToInclude.map(c => col(c)): _*)
      val optionsRowKeyCf = Map(HbaseConfigs.hbaseOperation -> "get", HbaseConfigs.hbaseFilter -> s"rowKey=1:toGet=$cfToGet")
      val readDFRowKeyCf = dataset.read(dataSet, optionsRowKeyCf)
      val countRowKeyCf = readDFRowKeyCf.cache.count()
      info(s"$tag | Read Count $countRowKeyCf...")
      info(s"$tag | Sample 10 Rows -->")
      readDFRowKeyCf.show(10)
      compareDataFrames(testDataRowKeyCf, readDFRowKeyCf)

      // Validating LookUp on rowKey+ColumnFamily+Column
      val columnToGet = columnsToInclude(0)
      info(s"$tag | Read from $dataSet... - Lookup on rowKey = 1, column family = " + cfToGet + " and column = " + columnToGet)
      val testDataRowKeyCfCol = testData.filter(col(rowKeyColumn) === "1").select(col(columnToGet))
      val optionsRowKeyCfCol = Map(HbaseConfigs.hbaseOperation -> "get", HbaseConfigs.hbaseFilter -> s"rowKey=1:toGet=$cfToGet-$columnToGet")
      val readDFRowKeyCfCol = dataset.read(dataSet, optionsRowKeyCfCol)
      val countRowKeyCfCol = readDFRowKeyCfCol.cache.count()
      info(s"$tag | Read Count $countRowKeyCfCol...")
      info(s"$tag | Sample 10 Rows -->")
      readDFRowKeyCfCol.show(10)
      compareDataFrames(testDataRowKeyCfCol, readDFRowKeyCfCol)

      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
    (ddls, stats, testData)
  }

  /**
    * Drops the HBASE Table in HBASE cluster
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpHBase() = withMethdNameLogging { methodName =>
    try {
      info("Dropping HBASE table --> ")
      storageadmin.HBaseAdminClient.deleteHbaseTable(
        gimelProps.hbaseNameSpace
        , gimelProps.smokeTestHBASETable
        , gimelProps.smokeTestHBASESiteXMLHDFS
      )
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Drops HBase Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpHBaseHive() = withMethdNameLogging { methodName =>
    try {
      val dropTableStatement = s"drop table if exists $dataSetName"
      sparkSession.sql(dropTableStatement)
      ddls += ("hbase_hive_ddl_drop" -> dropTableStatement)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }


}

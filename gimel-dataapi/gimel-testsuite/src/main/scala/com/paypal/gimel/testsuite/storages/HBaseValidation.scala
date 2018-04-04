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

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.common.storageadmin.HBaseAdminClient
import com.paypal.gimel.hive.utilities.HiveSchemaUtils
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class HBaseValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

  logger.info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestHBASEHiveTable}"

  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

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
  private def bootStrapHBase() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      cleanUpHBase()
      HBaseAdminClient.createHbaseTableIfNotExists(
        gimelProps.hbaseNameSpace
        , gimelProps.smokeTestHBASETable
        , gimelProps.smokeTestHBASETableColumnFamily.split(",")
        , gimelProps.smokeTestHBASESiteXMLHDFS
      )
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Prepare Dummy/Sample Data for SmokeTest
    *
    * @param numberOfRows Total Number of Sample Rows to Prep
    * @return DataFrame
    */
  override def prepareSmokeTestData(numberOfRows: Int = 1000): DataFrame = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      def stringed(n: Int) = s"""{"id": $n,"name": "MAC-$n", "address": "MAC-${n + 1}", "age": "${n + 1}", "company": "MAC-$n", "designation": "MAC-$n", "salary": "${n * 10000}" }"""

      val texts: Seq[String] = (1 to numberOfRows).map { x => stringed(x) }
      val rdd: RDD[String] = sparkSession.sparkContext.parallelize(texts)
      val dataFrameToWrite: DataFrame = sparkSession.read.json(rdd)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      dataFrameToWrite
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

    logger.info(" @Begin --> " + MethodName)

    try {
      val hbaseNameSpace: String = gimelProps.hbaseNameSpace
      val hbaseTableName: String = gimelProps.smokeTestHBASETable
      val hbaseHiveTableName: String = gimelProps.smokeTestHBASEHiveTable
      val hiveDB: String = gimelProps.smokeTestHiveDB
      val columnSet: Array[String] = gimelProps.smokeTestHBASETableColumns.split('|').flatMap(x => x.split(':')(1).split(','))
      val rowkeyColumn: String = gimelProps.smokeTestHBASETableRowKey
      val cfColsMap: Map[String, Array[String]] = gimelProps.smokeTestHBASETableColumns.split('|').map(x => (x.split(':')(0), x.split(':')(1).split(','))).toMap
      val hbaseDDL = HiveSchemaUtils.generateTableDDL(hiveDB, hbaseHiveTableName, hbaseNameSpace, hbaseTableName, columnSet, rowkeyColumn, cfColsMap).replaceAllLiterally(";", "")
      logger.info("Hbase Hive DDL: " + hbaseDDL)

      // we are adding these jars because Hive Session needs these jar for executing the above DDL(It needs hbase-hadoop jar for Hbase Handler)
      // we are not using hiveContext.sql because spark 2.1 version doesnt support Stored by Hbase Storage Handler.so we are replacing with Hive JDBC as it supports both versions(1.6 and 2.1)

      deployDDL(hbaseDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hbase" -> hbaseDDL)
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

    logger.info(" @Begin --> " + MethodName)

    try {
      logger.info("Dropping HBASE table --> ")
      storageadmin.HBaseAdminClient.deleteHbaseTable(
        gimelProps.hbaseNameSpace
        , gimelProps.smokeTestHBASETable
        , gimelProps.smokeTestHBASESiteXMLHDFS
      )
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Drops HBase Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpHBaseHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val dropTableStatement = s"drop table if exists $dataSetName"
      sparkSession.sql(dropTableStatement)
      ddls += ("hbase_hive_ddl_drop" -> dropTableStatement)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }
}

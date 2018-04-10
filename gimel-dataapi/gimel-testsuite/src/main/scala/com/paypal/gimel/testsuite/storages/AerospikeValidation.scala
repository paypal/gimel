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

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.DataSet
import com.paypal.gimel.aerospike.conf.AerospikeConfigs
import com.paypal.gimel.common.conf.{GimelConstants}
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class AerospikeValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

   info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestAerospikeHiveTable}"

  /**
    * CleanUp Aerospike Set & Hive Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

     info(" @Begin --> " + MethodName)

    cleanUpAerospike()
    cleanUpAerospikeHive()
    (ddls, stats)
  }

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapAerospikeHive()
  }

  /**
    * Creates Aeropsike Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapAerospikeHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

     info(" @Begin --> " + MethodName)

    try {
      val aerospikeNameSpace: String = gimelProps.smokeTestAerospikeNamespace
      val aerospikeSetName: String = gimelProps.smokeTestAerospikeSetName
      val aerospikeHiveTableName: String = gimelProps.smokeTestAerospikeHiveTable
      val aerospikeClient = gimelProps.smokeTestAerospikeClient
      val aerospikePort = gimelProps.smokeTestAerospikePort
      val hiveDB: String = gimelProps.smokeTestHiveDB
      val rowkeyColumn: String = gimelProps.smokeTestAerospikeRowKey
      val aerospikeDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS ${dataSetName}
           |(
           |`data` string COMMENT 'from deserializer'
           |)
           |TBLPROPERTIES (
           |  '${AerospikeConfigs.aerospikeNamespaceKey}'="${aerospikeNameSpace}",
           |  '${AerospikeConfigs.aerospikePortKey}'='${aerospikePort}',
           |  '${AerospikeConfigs.aerospikeSeedHostsKey}'='${aerospikeClient}',
           |  '${AerospikeConfigs.aerospikeSetKey}'='${aerospikeSetName}',
           |  '${GimelConstants.STORAGE_TYPE}'='AEROSPIKE')
           |""".stripMargin
       info("Aerospike Hive DDL: " + aerospikeDDL)

      deployDDL(aerospikeDDL)

      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_aerospike" -> aerospikeDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Main Place to implement Validation Steps
    *
    * @param testData DataFrame (Optional)
    * @return @return A Tuple of (DDL , STATS, Optional[DataFrame])
    */
  override def validateAPI(testData: Option[DataFrame] = None): (Map[String, String], Map[String, String], Option[DataFrame]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

     info(" @Begin --> " + MethodName)

    val storage = this.getClass.getName.replace(".", "_")
    val tag = s"$MethodName-$storage"
    try {
      val testData = prepareSmokeTestData(gimelProps.smokeTestSampleRowsCount.toInt)
      val dataSet = dataSetName
       info(s"$tag | Begin Write to $dataSet...")
      val options: Map[String, Any] = Map(AerospikeConfigs.aerospikeRowkeyKey -> gimelProps.smokeTestAerospikeRowKey)
      dataset.write(dataSet, testData, options)
       info(s"$tag | Write Success.")
       info(s"$tag | Read from $dataSet...")
      val readDF = dataset.read(dataSet)
      val count = readDF.count()
       info(s"$tag | Read Count $count...")
       info(s"$tag | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testData, readDF)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
    (ddls, stats, testData)
  }

  /**
    * Drops the Aerospike Set in Aerospike cluster
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpAerospike() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

     info(" @Begin --> " + MethodName)

    try {
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }

  /**
    * Drops Aerospike Hive Table Created to Test Data API - Read and Write
    */
  private def cleanUpAerospikeHive() = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

     info(" @Begin --> " + MethodName)

    try {
      val dropTableStatement = s"drop table if exists $dataSetName"
      sparkSession.sql(dropTableStatement)
      ddls += ("aerospike_hive_ddl_drop" -> dropTableStatement)
      stats += (s"$MethodName" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method $MethodName")
    }
  }
}

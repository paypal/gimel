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

import org.apache.spark.sql._

import com.paypal.gimel.DataSet
import com.paypal.gimel.common.storageadmin.JDBCAdminClient
import com.paypal.gimel.common.utilities.Timer
import com.paypal.gimel.jdbc.conf.{JdbcConfigs, JdbcConstants}
import com.paypal.gimel.jdbc.utilities.JDBCAuthUtilities
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class TeradataBulkLoadValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

   info(s"Initiated ${this.getClass.getName}")
  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestTeradataHiveTable}"
  val teradataTable = s"${gimelProps.smokeTestTeradataDB}.${gimelProps.smokeTestTeradataTable}"
  val url = s"jdbc:teradata://${gimelProps.smokeTestTeradataURL}"
  val dataSetProps: Map[String, Any] = Map((JdbcConstants.jdbcUserName, gimelProps.smokeTestTeradataUsername), (JdbcConfigs.jdbcP, gimelProps.smokeTestTeradataPFile))
  val authUtilities: JDBCAuthUtilities = JDBCAuthUtilities(sparkSession)
  val (username, password) = authUtilities.getJDBCCredentials(url, dataSetProps)
  val teradataURL: String = s"$url"
  val teradataWriteType: String = gimelProps.smokeTestTeradataWriteType
  val teradataReadType: String = gimelProps.smokeTestTeradataReadType
  val teradataSessions: String = gimelProps.smokeTestTeradataSessions
  val writeOptionsMap: Map[String, String] = Map(("TYPE", teradataWriteType), ("SESSIONS", teradataSessions), (JdbcConfigs.jdbcP, gimelProps.smokeTestTeradataPFile), (JdbcConstants.jdbcUserName, gimelProps.smokeTestTeradataUsername))
  val readOptionsMap: Map[String, String] = Map(("TYPE", teradataReadType), ("SESSIONS", teradataSessions), (JdbcConfigs.jdbcP, gimelProps.smokeTestTeradataPFile), (JdbcConstants.jdbcUserName, gimelProps.smokeTestTeradataUsername))

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapTeradata()
    bootStrapTeradataHive()
  }

  /**
    * Creates Teradata test table
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapTeradata() = withMethdNameLogging { methodName =>
    JDBCAdminClient.createTeradataTableIfNotExists(teradataURL, username, password, teradataTable)
  }

  /**
    * Creates Regular Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapTeradataHive() =withMethdNameLogging { methodName =>
    try {
      cleanUpTeradataHive()
      val hiveTableDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS `$dataSetName`
           |(
           | `id` int,
           | `name` string,
           | `rev` bigint
           |)
           |ROW FORMAT SERDE 'org.apache.hadoop.hive.jdbc.storagehandler.JdbcSerDe'
           |STORED BY '${JdbcConfigs.jdbcStorageHandler}'
           |WITH SERDEPROPERTIES ('serialization.format'='1')
           |TBLPROPERTIES (
           |'${JdbcConfigs.jdbcDriverClassKey}'='com.teradata.jdbc.TeraDriver',
           |'${JdbcConfigs.jdbcInputTableNameKey}'='$teradataTable',
           |'${JdbcConfigs.jdbcOutputTableNameKey}'='$teradataTable',
           |'${JdbcConfigs.jdbcUrl}'='jdbc:teradata://${gimelProps.smokeTestTeradataURL}'
           | )
      """.stripMargin

      info(s"DDLS -> $hiveTableDDL")
      deployDDL(hiveTableDDL)
      ddls += ("teradata_hive_ddl" -> hiveTableDDL)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * CleanUp
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = withMethdNameLogging { methodName =>
    cleanUpTeradata
    cleanUpTeradataHive()
    (ddls, stats)
  }

  private def cleanUpTeradata = withMethdNameLogging { methodName =>
    JDBCAdminClient.dropTeradataTableIfExists(teradataURL, username, password, teradataTable)

  }

  /**
    * Drops the Regular Hive Table used for testing DataSet.Read API
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpTeradataHive() = withMethdNameLogging { methodName =>
    try {
      val dropTableStatement = s"DROP TABLE IF EXISTS $dataSetName"
      sparkSession.sql(dropTableStatement)
      ddls += ("teradata_hive_ddl_drop" -> dropTableStatement)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
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
      val datasetWriteTimer = Timer()
      datasetWriteTimer.start
      dataset.write(dataSet, testData, writeOptionsMap)
       info(s"$tag | Write Success.")
      val datasetWriteTimeValue = datasetWriteTimer.endWithMillSecRunTime / 1000
       info(s"writeTime" -> s"$datasetWriteTimeValue")
       info(s"$tag | Read from $dataSet...")
      val datasetReadTimer = Timer()
      datasetReadTimer.start
      val readDF = dataset.read(dataSet, readOptionsMap)
      val count = readDF.count()
      val datasetReadTimeValue = datasetReadTimer.endWithMillSecRunTime / 1000
       info(s"readTime" -> s"$datasetReadTimeValue")
       info(s"$tag | Read Count $count...")
       info(s"$tag | Sample 10 Rows -->")
      readDF.show(10)
      compareDataFrames(testData, readDF)
      stats += (s"$tag" -> s"Success @ ${Calendar.getInstance.getTime}")
    } catch {
      case ex: Throwable =>
        stats += (s"$tag" -> s"Failure @ ${Calendar.getInstance.getTime}")
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
    (ddls, stats, testData)
  }
}

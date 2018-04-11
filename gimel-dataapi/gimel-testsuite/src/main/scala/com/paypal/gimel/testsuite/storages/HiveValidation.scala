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
import com.paypal.gimel.common.storageadmin.HDFSAdminClient
import com.paypal.gimel.hive.utilities.HiveSchemaUtils
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties

class HiveValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

  info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestHiveTable}"

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
    * Drops the Regular Hive Table used for testing DataSet.Read API
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpHive() = withMethdNameLogging { methodName =>
    try {
      val dropTableDDL = s"drop table if exists ${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestHiveTable}"
      sparkSession.sql(dropTableDDL)
      HDFSAdminClient.deletePath(s"${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestHiveTable}", true)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("Hive_Table_Drop_DDL" -> dropTableDDL)
      ddls += ("Hive_Table_Delete_HDFS" -> s"${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestHiveTable}")
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Creates Regular Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapHive = withMethdNameLogging { methodName =>
    try {
      val sampleDF = prepareSmokeTestData()
      val columnSet: Array[String] = sampleDF.columns
      val hiveDDL = HiveSchemaUtils.generateTableDDL("external"
        , gimelProps.smokeTestHiveDB
        , gimelProps.smokeTestHiveTable
        , s"${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestHiveTable}"
        , "parquet"
        , columnSet)
      deployDDL(hiveDDL)

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_hive" -> hiveDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }
}

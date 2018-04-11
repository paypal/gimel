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
import com.paypal.gimel.common.conf.{GimelConstants}
import com.paypal.gimel.common.storageadmin
import com.paypal.gimel.elasticsearch.conf.ElasticSearchConfigs
import com.paypal.gimel.testsuite.utilities.GimelTestSuiteProperties


class ElasticSearchValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties)
  extends StorageValidation(dataset: DataSet, sparkSession: SparkSession, gimelProps: GimelTestSuiteProperties) {

  info(s"Initiated ${this.getClass.getName}")

  val dataSetName = s"${gimelProps.smokeTestHiveDB}.${gimelProps.smokeTestESHiveTable}"

  /**
    * CleanUp ES Index & Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def cleanUp(): (Map[String, String], Map[String, String]) = {
    cleanUpESHive()
    val url = s"${gimelProps.esHost}:${gimelProps.esPort}/${gimelProps.smokeTestESIndex}"
    cleanUpES(url)
  }

  /**
    * BootStrap Required Storage Objects
    *
    * @return A Tuple of (DDL , STATS)
    */
  override def bootStrap(): (Map[String, String], Map[String, String]) = {
    bootStrapES()
  }

  /**
    * CleanUp ES Hive Table
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpESHive() = withMethdNameLogging { methodName =>
    try {
      val dropDDL = s"drop table if exists $dataSetName"
      sparkSession.sql(dropDDL)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("ES_Drop_Table" -> dropDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Delete ES Index
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def cleanUpES(url: String) = withMethdNameLogging { methodName =>
    info("delete index")
    info(url)
    try {
      val output = storageadmin.ESAdminClient.deleteIndex(url)
      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("ES_Index Dropped_Status" -> output)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }

  /**
    * Creates ES Hive Table to Test Data API - Read and Write
    *
    * @return A Tuple of (DDL , STATS)
    */
  private def bootStrapES() = withMethdNameLogging { methodName =>
    try {
      val typeName = gimelProps.tagToAdd.replace("_", "")
      val esDDL =
        s"""
           |CREATE EXTERNAL TABLE IF NOT EXISTS $dataSetName
           |(
           |  `data` string COMMENT 'from deserializer'
           |)
           |ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe'
           |STORED BY '${ElasticSearchConfigs.esStorageHandler}'
           |WITH SERDEPROPERTIES ('serialization.format'='1')
           |LOCATION
           |  '${gimelProps.smokeTestHiveLocation}/${gimelProps.smokeTestESIndex}'
           |TBLPROPERTIES (
           |  '${ElasticSearchConfigs.esIndexAutoCreate}'='true',
           |  '${GimelConstants.ES_NODE}'='${gimelProps.esHost}',
           |  '${GimelConstants.ES_PORT}'='${gimelProps.esPort}',
           |  '${ElasticSearchConfigs.esResource}'='${gimelProps.smokeTestESIndex}/$typeName'
           |)
      """.stripMargin

      // we are adding these jars because Hive Session needs these jar for executing the above DDL(It needs elasticsearch-hadoop jar for ESStorage Handler)
      // we are not using hiveContext.sql because spark 2.1 version doesnt support Stored by ES Storage Handler and Serde.so we are replacing with Hive JDBC as it supports both versions(1.6 and 2.1)

      deployDDL(esDDL)

      stats += (s"${methodName}" -> s"Success @ ${Calendar.getInstance.getTime}")
      ddls += ("DDL_es" -> esDDL)
      (ddls, stats)
    } catch {
      case ex: Throwable =>
        handleException(ex, s"Some Error While Executing Method ${methodName}")
    }
  }


}

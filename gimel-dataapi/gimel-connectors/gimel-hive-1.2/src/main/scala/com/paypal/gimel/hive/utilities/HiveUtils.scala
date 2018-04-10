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

package com.paypal.gimel.hive.utilities

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.CatalogProviderConstants
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.hdfs.conf.HdfsConfigs
import com.paypal.gimel.logger.Logger

class HiveUtils extends Logger {
  /**
    * write to hive table
    *
    * @param dataSetProps is options coming from the user
    * @param dataSet      - the hive table name
    * @param dataFrame    - dataframe that need to be inserted into the hive table
    * @param sparkSession : SparkSession
    * @return dataFrame
    */

  def write(dataSet: String, dataFrame: DataFrame, sparkSession: SparkSession, dataSetProps: Map[String, Any]): DataFrame = {

    val datasetProps: DataSetProperties = dataSetProps(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
    val saveMode = dataSetProps.getOrElse("saveMode", "").toString.toLowerCase
    val apiType = dataSetProps.getOrElse("apiType", "").toString.toLowerCase
    val location = datasetProps.props(CatalogProviderConstants.PROPS_LOCATION)
    val partitionColumns = datasetProps.partitionFields.map(_.fieldName)
    val partKeys = partitionColumns.mkString(",")
    val fieldNames = datasetProps.fields.map(_.fieldName).mkString(",")

    try {

      info("Registering temp table")
      dataFrame.registerTempTable("tempTable")
      info("Register temp table completed")

      val insertPrefix = saveMode.toUpperCase match {
        case "APPEND" =>
          "insert into"
        case "OVERWRITE" =>
          "insert overwrite table"
        case _ =>
          "insert into"
      }

      info(s"insertPrefix:$insertPrefix")
      info(s"saveMode:$saveMode, apiType:$apiType,location:$location, partKeys:$partKeys, partitionColumns:$partitionColumns, fieldNames:$fieldNames")

      val fnlInsert = {
        if (partKeys.isEmpty) {
          s"""
             |$insertPrefix $dataSet
             |select
             |$fieldNames
             | from tempTable
    """.stripMargin
        } else {
          s"""
             |$insertPrefix $dataSet
             | partition ($partKeys)
             |select
             |$fieldNames,$partKeys
             | from tempTable
            """.stripMargin
        }
      }

      info(s"final insert: $fnlInsert")
      info("Executing final insert statement")

      sparkSession.conf.set(HdfsConfigs.dynamicPartitionKey, "true")
      sparkSession.conf.set(HdfsConfigs.dynamicPartitionModeKey, "nonstrict")
      sparkSession.sql(fnlInsert)

      dataFrame
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

}

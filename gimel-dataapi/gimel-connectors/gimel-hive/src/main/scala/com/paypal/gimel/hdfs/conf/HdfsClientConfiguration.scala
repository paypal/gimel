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

package com.paypal.gimel.hdfs.conf

import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for HDFS Dataset Operations.
  *
  * @param props HDFS Client properties.
  */
class HdfsClientConfiguration(val props: Map[String, Any]) {


  val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  // Load Default Prop from Resource File
  val pcatProps = GimelProperties()

  // appTag is used to maintain checkpoints & various other factors that are unique to the application
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props
  val inferSchema = props.getOrElse(HdfsConfigs.inferSchemaKey, "true").toString
  val header = props.getOrElse(HdfsConfigs.fileHeaderKey, "true").toString
  val rowDelimiter = tableProps.getOrElse(HdfsConfigs.rowDelimiter, HdfsConstants.newLineDelimiterValue).toString

  // Column delimiter for CSV
  // For Backward Compatibility
  val hiveCsvFieldDelimiter = tableProps.getOrElse(HdfsConfigs.hiveFieldDelimiterKey, HdfsConstants.commaDelimiterValue)
  val csvDelimiterV1 = tableProps.getOrElse(HdfsConfigs.columnDelimiterVersion1, hiveCsvFieldDelimiter).toString
  val csvDelimiter = tableProps.getOrElse(HdfsConfigs.columnDelimiter, csvDelimiterV1).toString
  // Column delimiter for other formats
  // For Backward Compatibility
  val hiveFieldDelimiter = tableProps.getOrElse(HdfsConfigs.hiveFieldDelimiterKey, HdfsConstants.controlAOctalDelimiterValue)
  val colDelimiterV1 = tableProps.getOrElse(HdfsConfigs.columnDelimiterVersion1, hiveFieldDelimiter).toString
  val colDelimiter = tableProps.getOrElse(HdfsConfigs.columnDelimiter, colDelimiterV1).toString

  val clusterNameNode = tableProps.getOrElse(GimelConstants.hdfsNameNodeKey, "")
  val clusterDataLocation = tableProps.getOrElse(HdfsConfigs.hdfsDataLocationKey, "")
  val clusterdataFormat = tableProps.getOrElse(HdfsConfigs.hdfsDataFormatKey, "text")
  val clusterThresholdGB = props.getOrElse(HdfsConfigs.hdfsCrossClusterThresholdKey, HdfsConstants.thresholdGBData).toString
  val readOptions = props.getOrElse(HdfsConfigs.readOptions, "").toString

  logger.info(s"Hdfs Props --> ${tableProps.map(x => s"${x._1} --> ${x._2}").mkString("\n")}")
  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")
}

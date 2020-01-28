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

package com.paypal.gimel.hbase.conf

import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.{GimelConstants, GimelProperties}
import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

/**
  * Gimel Client Configuration for Hbase Dataset Operations.
  *
  * @param props Hbase Client properties.
  */
class HbaseClientConfiguration(val props: Map[String, Any]) {

  private val logger = Logger()
  logger.info(s"Begin Building --> ${this.getClass.getName}")
  //  logger.info(s"Incoming Properties --> ${props.map(x => s"${x._1} -> ${x._2}").mkString("\n")}")

  // Load Default Prop from Resource File
  val pcatProps = GimelProperties()

  // appTag is used to maintain checkpoints & various other factors that are unique to the application
  val appTag: String = props.getOrElse(GimelConstants.APP_TAG, "").toString

  // This is the DataSet Properties
  val datasetProps: DataSetProperties = props(GimelConstants.DATASET_PROPS).asInstanceOf[DataSetProperties]
  val tableProps: Map[String, String] = datasetProps.props

  val clusterName = com.paypal.gimel.common.utilities.DataSetUtils.getYarnClusterName()
  val hbaseNameSpaceAndTable = GenericUtils.getValueFailIfEmpty(tableProps, HbaseConfigs.hbaseTableKey,
    "HBase table name not found. Please set the property " + HbaseConfigs.hbaseTableKey)
  val hbaseTableColumnMapping = tableProps.getOrElse(HbaseConfigs.hbaseColumnMappingKey, "")
  val maxSampleRecordsForSchema = GenericUtils.getValue(tableProps, HbaseConfigs.hbaseMaxRecordsForSchema, HbaseConstants.MAX_SAMPLE_RECORDS_FOR_SCHEMA).toInt
  val maxColumnsForSchema = GenericUtils.getValue(tableProps, HbaseConfigs.hbaseMaxColumnsForSchema, HbaseConstants.MAX_COLUMNS_FOR_SCHEMA).toInt
  // If this property consists of namespace and tablename both separated by colon ":", take the table name by splitting this string
  val hbaseTableNamespaceSplit = hbaseNameSpaceAndTable.split(":")
  val hbaseTableName = if (hbaseTableNamespaceSplit.length > 1) {
    hbaseTableNamespaceSplit(1)
  } else {
    hbaseNameSpaceAndTable
  }
  val hbaseNameSpace = tableProps.getOrElse(HbaseConfigs.hbaseNamespaceKey, HbaseConstants.DEFAULT_NAMESPACE)
  // If ColumnFamily name needs to be appneded with Column Name in resultant Dataframe
  val hbaseColumnNamewithColumnFamilyAppended = tableProps.getOrElse(HbaseConfigs.hbaseColumnNamewithColumnFamilyAppended, "false").toString.toBoolean
  // HDFS path for hbase-site.xml
  val hbaseSiteXMLHDFSPath = tableProps.getOrElse(HbaseConfigs.hbaseSiteXMLHDFSPathKey, HbaseConstants.NONE_STRING)
  val schema: Array[String] = if (datasetProps.fields != null && datasetProps.fields.nonEmpty) {
    datasetProps.fields.map(_.fieldName)
  } else {
    Array.empty[String]
  }

  val getOption = tableProps.getOrElse(HbaseConfigs.hbaseFilter, "")

  // Getting Row Key from user otherwise from schema in UDC or hive table. If it is not present in schema also, set defaultValue
  val hbaseRowKeys = tableProps.getOrElse(HbaseConfigs.hbaseRowKey, HbaseConstants.DEFAULT_ROW_KEY_COLUMN).split(",")

  logger.info(s"Fields Initiated --> ${this.getClass.getFields.map(f => s"${f.getName} --> ${f.get().toString}").mkString("\n")}")
  logger.info(s"Completed Building --> ${this.getClass.getName}")

}


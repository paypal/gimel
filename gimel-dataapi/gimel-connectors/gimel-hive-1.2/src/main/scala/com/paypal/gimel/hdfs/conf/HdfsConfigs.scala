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

// CSV  and HDFS Data API Configuration
object HdfsConfigs {
  // Gimel configs
  val hdfsDataLocationKey = "gimel.hdfs.data.location"
  val hdfsDataFormatKey = "gimel.hdfs.data.format"
  val saveModeInfo = "gimel.hdfs.save.mode"
  val rowDelimiter = "gimel.fs.row.delimiter"
  val columnDelimiter = "gimel.fs.column.delimiter"
  val columnDelimiterVersion1 = "gimel.hdfs.column.delimiter"
  val inferSchemaKey = "gimel.hdfs.csv.data.inferSchema"
  val fileHeaderKey = "gimel.hdfs.csv.data.headerProvided"
  val compressionCodecKey = "gimel.hdfs.data.compressionCodec"
  val hdfsCrossClusterThresholdKey = "gimel.hdfs.data.crosscluster.threshold"
  val hiveDatabaseName : String = "gimel.hive.db.name"
  val hiveTableName: String = "gimel.hive.table.name"
  val readOptions : String = "gimel.hdfs.read.options"

  // Other configs
  val fileOutputFormatCompressionFlag: String = "mapreduce.output.fileoutputformat.compress"
  val sparkSqlCompressionCodec: String = "spark.sql.parquet.compression.codec"
  val dynamicPartitionKey: String = "hive.exec.dynamic.partition"
  val dynamicPartitionModeKey: String = "hive.exec.dynamic.partition.mode"
  val hiveFieldDelimiterKey: String = "field.delim"
}

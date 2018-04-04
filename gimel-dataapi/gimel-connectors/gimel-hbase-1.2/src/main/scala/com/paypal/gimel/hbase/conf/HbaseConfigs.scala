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

// keys related to HBASE
object HbaseConfigs {
  // metastore properties
  val hbaseTableKey: String = "gimel.hbase.table.name"
  val hbaseColumnMappingKey: String = "gimel.hbase.columns.mapping"
  // misc properties for read/write
  val hbaseStorageHandler: String = "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
  val hbaseOperation: String = "gimel.hbase.operation"
  val hbaseFilter: String = "gimel.hbase.get.filter"
  val hbaseRowKey: String = "gimel.hbase.rowkey"
  // polling properties
  val hbaseScanMaxResultsKey: String = "gimel.hbase.scan.max.results"
  val hbaseSiteXMLPathKey: String = "gimel.hbase.site.xml.path"
  val hbaseSiteXMLHDFSPathKey: String = "gimel.hbase.site.xml.hdfs.path"
  val hbaseUseColumnsSpecifiedFlag: String = "gimel.hbase.columns.specified.flag"
  val hbasePollingBlackListTables: String = "chufan_indexing,hbase,nrt_network_another_1_0,sre_mm,test"
  val hbasePollingBlackListKey: String = "gimel.hbase.polling.blacklist"
}


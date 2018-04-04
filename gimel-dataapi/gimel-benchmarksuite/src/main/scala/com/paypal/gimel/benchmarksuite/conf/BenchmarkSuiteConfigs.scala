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

package com.paypal.gimel.benchmarksuite.conf

object BenchmarkSuiteConfigs {
  // basic variables
  // IMPORTANT: Keys changed here must be changed in benchmark.properties as well
  val sampleRowCount: String = "benchmark.gimel.sample.rows.count"
  val kafkaCDHHiveTableKey: String = "benchmark.kafka.cdh.hive.table"
  val benchMarkTestCDHKafkaTopicKey: String = "benchmark.kafka.cdh.topic"
  val benchMarkTestKafkaDatasetHiveTableKey: String = "benchmark.kafka.dataset.hive.table"
  val benchMarkTestKafkaTopicDatasetKey: String = "benchmark.kafka.dataset.topic"
  val benchMarkTestKafkaTopicNativeKey: String = "benchmark.kafka.native.topic"
  val benchMarkTestHbaseNamespaceKey: String = "benchmark.hbase.namespace"
  val benchMarkTestHBASETableRowKey: String = "benchmark.hbase.table.row.key"
  val benchMarkTestHBASETableColumnFamilyKey: String = "benchmark.hbase.table.columnfamily"
  val benchMarkTestHBASEHiveTableDatasetKey: String = "benchmark.hbase.dataset.hive.table"
  val benchMarkTestHBASEHiveTableNativeKey: String = "benchmark.hbase.native.hive.table"
  val benchMarkTestHBASETableDatasetKey: String = "benchmark.hbase.dataset.table"
  val benchMarkTestHBASETableNativeKey: String = "benchmark.hbase.native.table"
  val benchMarkTestHBASESiteXMLHDFSKey: String = "benchmark.hbase.site.xml.hdfs"
  val benchMarkTestHBASESiteXMLLocalKey: String = "benchmark.hbase.site.xml.local"
  val benchMarkTestESHiveTableKey: String = "benchmark.es.dataset.hive.table"
  val benchMarkTestESDatasetIndexKey: String = "benchmark.es.dataset.index"
  val benchMarkTestESNativeIndexKey: String = "benchmark.es.native.index"
  val benchMarkTestHiveTableNativeAPIKey: String = "benchmark.hive.native.table"
  val benchMarkTestHiveTableDatasetAPIKey: String = "benchmark.hive.dataset.table"
  val benchMarkTestHiveDBKey: String = "benchmark.gimel.hive.db"
  val benchMarkTestHiveLocationKey: String = "benchmark.gimel.hive.location"
  val benchmarkTestHiveFormatKey: String = "benchmark.hive.table.format"
  val benchMarkTestSampleRowsCountKey: String = "benchmark.gimel.sample.rows.count"
  val benchMarkTestResultEsHostKey: String = "benchmark.result.stats.es.host"
  val benchMarkTestResultEsPortKey: String = "benchmark.result.stats.es.port"
  val benchMarkTestResultEsIndexKey: String = "benchmark.result.stats.es.index"
}


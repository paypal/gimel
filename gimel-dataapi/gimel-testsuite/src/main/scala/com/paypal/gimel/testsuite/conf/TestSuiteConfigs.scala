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

package com.paypal.gimel.testsuite.conf

import com.paypal.gimel.common.conf.GimelConstants

object TestSuiteConfigs {
  // IMPORTANT: Keys changed here must be changed in gimel.properties as well
  val smokeTestCDHKafkaHiveTableKey: String = "smoketest.kafka.cdh.hive.table"
  val smokeTestCDHKafkaTopicKey: String = "smoketest.kafka.cdh.topic"
  val smokeTestKafkaHiveTableKey: String = "smoketest.kafka.hive.table"
  val smokeTestKafkaTopicKey: String = "smoketest.kafka.topic"
  val smokeTestHBASENameSpaceKey: String = "smoketest.hbase.namespace"
  val smokeTestHBASETableRowKey: String = "smoketest.hbase.table.row.key"
  val smokeTestHBASETableColumnFamilyKey: String = "smoketest.hbase.table.columnfamily"
  val smokeTestHBASETableColumnsKey: String = "smoketest.hbase.table.columns"
  val smokeTestHBASEHiveTableKey: String = "smoketest.hbase.hive.table"
  val smokeTestHBASETableKey: String = "smoketest.hbase.table"
  val smokeTestHBASESiteXMLHDFSKey: String = "smoketest.hbase.site.xml.hdfs"
  val smokeTestHBASESiteXMLLocalKey: String = "smoketest.hbase.site.xml.local"
  val smokeTestESHiveTableKey: String = "smoketest.es.hive.table"
  val smokeTestESIndexKey: String = "smoketest.es.index"
  val smokeTestHiveTableKey: String = "smoketest.hive.table"
  val smokeTestHiveDBKey: String = "smoketest.gimel.hive.db"
  val smokeTestHiveLocationKey: String = "smoketest.gimel.hive.location"
  val smokeTestTeradataTableKey: String = "smoketest.teradata.table"
  val smokeTestTeradataDBKey: String = "smoketest.teradata.db"
  val smokeTestTeradataHiveTableKey: String = "smoketest.gimel.hive.table"
  val smokeTestTeradataUsernameKey: String = s"smoketest.teradata.${GimelConstants.USER_NAME}"
  val smokeTestTeradataPFileKey: String = "smoketest.teradata.p.file"
  val smokeTestTeradataURLKey: String = "smoketest.teradata.url"
  val smokeTestTeradataReadTypeKey: String = "smoketest.teradata.read.type"
  val smokeTestTeradataWriteTypeKey: String = "smoketest.teradata.write.type"
  val smokeTestTeradataSessionsKey: String = "smoketest.teradata.sessions"
  val smokeTestTeradataBatchSizeKey: String = "smoketest.teradata.batchsize"
  val smokeTestCDHKafkaStreamHiveTableKey: String = "smoketest.kafka.stream.hive.table"
  val smokeTestKafkaStreamESHiveTableKey: String = "smoketest.kafka.stream.es.hive.table"
  val smokeTestKafkaStreamESIndexKey: String = "smoketest.kafka.stream.es.index"
  val smokeTestKafkaStreamBatchIntervalKey: String = "smoketest.kafka.stream.batch.interval"
  val smokeTestStreamingAwaitTerminationKey: String = "smoketest.gimel.streaming.awaitTerminationOrTimeout"
  val smokeTestSampleRowsCountKey: String = "smoketest.gimel.sample.rows.count"
  val smokeTestResultEsHostKey: String = "smoketest.result.stats.es.host"
  val smokeTestResultEsPortKey: String = "smoketest.result.stats.es.port"
  val smokeTestResultEsIndexKey: String = "smoketest.result.stats.es.index"
  val smokeTestAerospikeHiveTableKey: String = "smoketest.aerospike.hive.table"
  val smokeTestAerospikeSetNameKey: String = "smoketest.aerospike.set.name"
  val smokeTestAerospikeNamespaceKey: String = "smoketest.aerospike.namespace.name"
  val smokeTestAerospikeClientKey: String = "smoketest.aerospike.client"
  val smokeTestAerospikePortKey: String = "smoketest.aerospike.port"
  val smokeTestAerospikeRowKeyKey: String = "smoketest.aerospike.rowkey"
  val pcatalogQueryMonitorESDataSetKey: String = "gimel.query.monitor.stats.es.dataset"
  val pcatalogQueryMonitorESIndexKey: String = "gimel.query.monitor.stats.es.index"
  val pcatalogQueryMonitorESHostKey: String = "gimel.query.monitor.stats.es.host"
  val pcatalogQueryMonitorESPortKey: String = "gimel.query.monitor.stats.es.port"
  val pcatalogQueryMonitorKafkaDataSetKey: String = "gimel.query.monitor.stats.kafka.dataset"
  val pcatalogQueryMonitorKafkaTopicKey: String = "gimel.query.monitor.stats.kafka.topic"
  val pcatalogQueryMonitorKafkaBrokerKey: String = "gimel.query.monitor.stats.kafka.broker"
  val pcatalogQueryMonitorKafkaZKKey: String = "gimel.query.monitor.stats.zookeeper.host"
  val pcatalogHiveJarsToAdd: String = "gimel.hiveJars.ddl"
}


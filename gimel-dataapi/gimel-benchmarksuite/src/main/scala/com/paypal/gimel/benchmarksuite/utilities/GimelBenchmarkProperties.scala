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

package com.paypal.gimel.benchmarksuite.utilities

import java.util.{Calendar, Properties}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable

import com.paypal.gimel.benchmarksuite.conf.{BenchmarkSuiteConfigs, BenchmarkSuiteConstants}
import com.paypal.gimel.common.conf._
import com.paypal.gimel.kafka.conf.KafkaConfigs
import com.paypal.gimel.logger.Logger

class GimelBenchmarkProperties(userProps: Map[String, String] = Map[String, String]()) extends Logger {
  // Get Properties
  val props: mutable.Map[String, String] = getProps
  val runTagUUID: String = java.util.UUID.randomUUID.toString
  val startTimeMS: String = Calendar.getInstance().getTimeInMillis.toString
  val tagToAdd: String = s"_$startTimeMS"
  val tagId: String = s"$startTimeMS"
  val cluster: String = userProps.getOrElse(BenchmarkSuiteConstants.clusterNameKey, "Unknown")

  private def getConf(key: String): String = {
    userProps.getOrElse(key, props(key))
  }

  // Kafka Properties
  val kafkaBroker: String = getConf(GimelConstants.KAFKA_BROKER_LIST)
  val kafkaConsumerCheckPointRoot: String = getConf(GimelConstants.KAFKA_CONSUMER_CHECKPOINT_PATH)
  val kafkaAvroSchemaKey: String = getConf(GimelConstants.KAFKA_CDH_SCHEMA)
  val confluentSchemaURL: String = getConf(GimelConstants.CONFLUENT_SCHEMA_URL)
  val hbaseNameSpace: String = getConf(GimelConstants.HBASE_NAMESPACE)
  val zkHostAndPort: String = getConf(GimelConstants.ZOOKEEPER_LIST)
  val esHost: String = getConf(GimelConstants.ES_NODE)
  val esPort: String = getConf(GimelConstants.ES_PORT)

  // All TestSuit Props for Kafka
  val benchMarkTestCDHKafkaHiveTable: String = getConf(BenchmarkSuiteConfigs.kafkaCDHHiveTableKey)
  val benchMarkTestCDHKafkaTopic: String = getConf(BenchmarkSuiteConfigs.benchMarkTestCDHKafkaTopicKey)
  val benchMarkTestKafkaDatasetHiveTable: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestKafkaDatasetHiveTableKey) + tagToAdd
  val benchMarkTestKafkaTopic_Dataset: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestKafkaTopicDatasetKey) + tagToAdd
  val benchMarkTestKafkaTopic_Native: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestKafkaTopicNativeKey) + tagToAdd

  // Test Suit Props for HBASE
  val benchMarkTestHBASENameSpace: String = getConf(BenchmarkSuiteConfigs.benchMarkTestHbaseNamespaceKey)
  val benchMarkTestHBASETableRowKey: String = getConf(BenchmarkSuiteConfigs.benchMarkTestHBASETableRowKey)
  val benchMarkTestHBASETableColumnFamily: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASETableColumnFamilyKey)
  val benchMarkTestHBASEHiveTable_Dataset: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASEHiveTableDatasetKey) + tagToAdd
  val benchMarkTestHBASEHiveTable_Native: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASEHiveTableNativeKey) + tagToAdd
  val benchMarkTestHBASETable_Dataset: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASETableDatasetKey) + tagToAdd
  val benchMarkTestHBASETable_Native: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASETableNativeKey) + tagToAdd
  val benchMarkTestHBASESiteXMLHDFS: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASESiteXMLHDFSKey)
  val benchMarkTestHBASESiteXMLLocal: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHBASESiteXMLLocalKey)

  // Test Suit Props for ES
  val benchMarkTestESHiveTable: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestESHiveTableKey) + tagToAdd
  val benchMarkTestESDatasetIndex: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestESDatasetIndexKey) + tagToAdd
  val benchMarkTestESNativeIndex: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestESNativeIndexKey) + tagToAdd

  // Test Suit Props for Hive
  val benchMarkTestHiveTable_NativeAPI: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHiveTableNativeAPIKey) + tagToAdd
  val benchMarkTestHiveTable_DatasetAPI: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHiveTableDatasetAPIKey) + tagToAdd
  val benchMarkTestHiveDB: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHiveDBKey) + tagToAdd
  val benchMarkTestHiveLocation: String =
    getConf(BenchmarkSuiteConfigs.benchMarkTestHiveLocationKey) + tagToAdd
  val benchmarkTestHiveFormat: String =
    getConf(BenchmarkSuiteConfigs.benchmarkTestHiveFormatKey)

  // Test Suit Props for Stats
  val benchMarkTestSampleRowsCount: String = getConf(BenchmarkSuiteConfigs.benchMarkTestSampleRowsCountKey)
  val benchMarkTestResultEsHost: String = getConf(BenchmarkSuiteConfigs.benchMarkTestResultEsHostKey)
  val benchMarkTestResultEsPort: String = getConf(BenchmarkSuiteConfigs.benchMarkTestResultEsPortKey)
  val benchMarkTestResultEsIndex: String = getConf(BenchmarkSuiteConfigs.benchMarkTestResultEsIndexKey)


  //  Kafka Serde
  val kafkaKeySerializer: String = KafkaConfigs.kafkaStringSerializer
  val kafkaValueSerializer: String = KafkaConfigs.kafkaStringSerializer

  // Kafka Consumer Props
  val KafkaConsumerGroupID: String = 111.toString
  val kafkaZKTimeOutMilliSec: String = 10000.toString
  val kafkaAutoOffsetReset: String = "smallest"
  val zkCheckPoint: String = kafkaConsumerCheckPointRoot + "/" + benchMarkTestKafkaTopic_Native


  // Explicitly Making a Map of Properties that are necessary
  // to Connect to Kafka for Publishes (Writes)
  val kafkaProducerProps: Properties = new java.util.Properties()
  scala.collection.immutable.Map(KafkaConfigs.kafkaServerKey -> kafkaBroker
    , KafkaConfigs.serializerKey -> kafkaKeySerializer
    , KafkaConfigs.serializerValue -> kafkaValueSerializer
    , KafkaConfigs.kafkaTopicKey -> benchMarkTestKafkaTopic_Native).foreach { kvPair =>
    kafkaProducerProps.put(kvPair._1.toString, kvPair._2.toString)
  }


  // Explicitly Making a Map of Properties that are necessary
  // to Connect to Kafka for Subscribes (Reads)
  // val kafkaConsumerProps: Map[String, String] =
  //   scala.collection.immutable.Map("bootstrap.servers" -> kafkaBroker
  //  , "group.id" -> KafkaConsumerGroupID
  //  , "zookeeper.connection.timeout.ms" -> kafkaZKTimeOutMilliSec
  //  , "auto.offset.reset" -> kafkaAutoOffsetReset
  //  , "kafka.topic" -> benchMarkTestKafkaTopic_Native)


  /**
    * Returns Properties from the resources file
    *
    * @return mutable.Map[String, String]
    */
  private def getProps: mutable.Map[String, String] = {
    val props: Properties = new Properties()
    val configStream = this.getClass.getResourceAsStream("/benchmark.properties")
    props.load(configStream)
    configStream.close()
    val finalProps: mutable.Map[String, String] = mutable.Map(props.asScala.toSeq: _*)
    info("PCatalog BenchMark Properties -->")
    finalProps.foreach(prop => info(prop))
    finalProps
  }
}

/**
  * Companion Object
  */
object GimelBenchmarkProperties {

  /**
    * If nothing is supplied from User ; Load all props from file in resources folder
    *
    * @return PCatalogProperties
    */
  def apply(): GimelBenchmarkProperties = new GimelBenchmarkProperties()

  /**
    * Use the properties supplied by user & load the defaults from resources where-ever applicable
    *
    * @param params User Supplied properties as a KV Pair
    * @return PCatalogProperties
    */
  def apply(params: Map[String, String]): GimelBenchmarkProperties = {
    new GimelBenchmarkProperties(params)
  }

}

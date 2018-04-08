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

package com.paypal.gimel.testsuite.utilities

import java.util.{Calendar, Properties}

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.logger.Logger
import com.paypal.gimel.testsuite.conf.TestSuiteConfigs

class GimelTestSuiteProperties(userProps: Map[String, String] = Map[String, String]()) {
  // Get Logger
  val logger = Logger()
  logger.info(s"Initiating --> ${this.getClass.getName}")
  // Get Properties
  val props: mutable.Map[String, String] = getProps
  val runTagUUID: String = java.util.UUID.randomUUID.toString
  val startTimeMS: String = Calendar.getInstance().getTimeInMillis.toString
  val tagToAdd: String = s"_$startTimeMS"

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
  val zkPrefix: String = getConf(GimelConstants.ZOOKEEPER_STATE)
  val esHost: String = getConf(GimelConstants.ES_NODE)
  val esPort: String = getConf(GimelConstants.ES_PORT)

  // Kerberos
  val keytab: String = getConf(GimelConstants.KEY_TAB)
  val principal: String = getConf(GimelConstants.KEY_TAB_PRINCIPAL)
  val cluster: String = getConf(GimelConstants.CLUSTER)
  val dataSetDeploymentClusters: String = getConf(GimelConstants.DEPLOYMENT_CLUSTERS)

  // All TestSuite Props for Kafka
  val smokeTestCDHKafkaHiveTable: String = getConf(TestSuiteConfigs.smokeTestCDHKafkaHiveTableKey)
  val smokeTestCDHKafkaTopic: String = getConf(TestSuiteConfigs.smokeTestCDHKafkaTopicKey)
  val smokeTestKafkaHiveTable: String = getConf(TestSuiteConfigs.smokeTestKafkaHiveTableKey) + tagToAdd
  val smokeTestKafkaTopic: String = getConf(TestSuiteConfigs.smokeTestKafkaTopicKey) + "_" + cluster + tagToAdd

  // TestSuite Props for HBASE
  val smokeTestHBASENameSpace: String = getConf(TestSuiteConfigs.smokeTestHBASENameSpaceKey)
  val smokeTestHBASETableRowKey: String = getConf(TestSuiteConfigs.smokeTestHBASETableRowKey)
  val smokeTestHBASETableColumnFamily: String = getConf(TestSuiteConfigs.smokeTestHBASETableColumnFamilyKey)
  val smokeTestHBASETableColumns: String = getConf(TestSuiteConfigs.smokeTestHBASETableColumnsKey)
  val smokeTestHBASEHiveTable: String = getConf(TestSuiteConfigs.smokeTestHBASEHiveTableKey) + tagToAdd
  val smokeTestHBASETable: String = getConf(TestSuiteConfigs.smokeTestHBASETableKey) + "_" + cluster + tagToAdd
  val smokeTestHBASESiteXMLHDFS: String = getConf(TestSuiteConfigs.smokeTestHBASESiteXMLHDFSKey)
  val smokeTestHBASESiteXMLLocal: String = getConf(TestSuiteConfigs.smokeTestHBASESiteXMLLocalKey)

  // Test Suit Props for ES
  val smokeTestESHiveTable: String = getConf(TestSuiteConfigs.smokeTestESHiveTableKey) + tagToAdd
  val smokeTestESIndex: String = getConf(TestSuiteConfigs.smokeTestESIndexKey) + "_" + cluster

  // Test Suit Props for Hive
  val smokeTestHiveTable: String = getConf(TestSuiteConfigs.smokeTestHiveTableKey) + tagToAdd
  val smokeTestHiveDB: String = getConf(TestSuiteConfigs.smokeTestHiveDBKey)
  val smokeTestHiveLocation: String = getConf(TestSuiteConfigs.smokeTestHiveLocationKey) + tagToAdd
  val gimelHiveJarsToAdd: String = getConf(TestSuiteConfigs.pcatalogHiveJarsToAdd)

  // Test Suit Props for JDBC(Teradata)
  val smokeTestTeradataTable: String = getConf(TestSuiteConfigs.smokeTestTeradataTableKey) + "_" + cluster + tagToAdd
  val smokeTestTeradataDB: String = getConf(TestSuiteConfigs.smokeTestTeradataDBKey)
  val smokeTestTeradataHiveTable: String = getConf(TestSuiteConfigs.smokeTestTeradataHiveTableKey) + tagToAdd
  val smokeTestTeradataUsername: String = getConf(TestSuiteConfigs.smokeTestTeradataUsernameKey)
  val smokeTestTeradataPFile: String = getConf(TestSuiteConfigs.smokeTestTeradataPFileKey)
  val smokeTestTeradataURL: String = getConf(TestSuiteConfigs.smokeTestTeradataURLKey)
  val smokeTestTeradataReadType: String = getConf(TestSuiteConfigs.smokeTestTeradataReadTypeKey)
  val smokeTestTeradataWriteType: String = getConf(TestSuiteConfigs.smokeTestTeradataWriteTypeKey)
  val smokeTestTeradataSessions: String = getConf(TestSuiteConfigs.smokeTestTeradataSessionsKey)
  val smokeTestTeradataBatchSize: String = getConf(TestSuiteConfigs.smokeTestTeradataBatchSizeKey)

  // Test Suit Props for Stats
  val smokeTestSampleRowsCount: String = getConf(TestSuiteConfigs.smokeTestSampleRowsCountKey)
  val smokeTestResultEsHost: String = getConf(TestSuiteConfigs.smokeTestResultEsHostKey)
  val smokeTestResultEsPort: String = getConf(TestSuiteConfigs.smokeTestResultEsPortKey)
  val smokeTestResultEsIndex: String = getConf(TestSuiteConfigs.smokeTestResultEsIndexKey)

  // Test Suit Props for Aerospike
  val smokeTestAerospikeHiveTable: String = getConf(TestSuiteConfigs.smokeTestAerospikeHiveTableKey) + tagToAdd
  val smokeTestAerospikeSetName: String = getConf(TestSuiteConfigs.smokeTestAerospikeSetNameKey) + tagToAdd
  val smokeTestAerospikeNamespace: String = getConf(TestSuiteConfigs.smokeTestAerospikeNamespaceKey)
  val smokeTestAerospikeClient: String = getConf(TestSuiteConfigs.smokeTestAerospikeClientKey)
  val smokeTestAerospikePort: String = getConf(TestSuiteConfigs.smokeTestAerospikePortKey)
  val smokeTestAerospikeRowKey: String = getConf(TestSuiteConfigs.smokeTestAerospikeRowKeyKey)

  // QueryMonitor
  val pcatalogQueryMonitorESDataSet: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorESDataSetKey)
  val pcatalogQueryMonitorESIndex: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorESIndexKey)
  val pcatalogQueryMonitorESHost: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorESHostKey)
  val pcatalogQueryMonitorESPort: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorESPortKey)
  val pcatalogQueryMonitorKafkaDataSet: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorKafkaDataSetKey)
  val pcatalogQueryMonitorKafkaTopic: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorKafkaTopicKey)
  val pcatalogQueryMonitorKafkaBroker: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorKafkaBrokerKey)
  val pcatalogQueryMonitorKafkaZK: String = getConf(TestSuiteConfigs.pcatalogQueryMonitorKafkaZKKey)

  // kafka streaming
  val smokeTestCDHKafkaStreamHiveTable: String = getConf(TestSuiteConfigs.smokeTestCDHKafkaStreamHiveTableKey) + tagToAdd
  val smokeTestKafkaStreamESHiveTable: String = getConf(TestSuiteConfigs.smokeTestKafkaStreamESHiveTableKey) + tagToAdd
  val smokeTestKafkaStreamESIndex: String = getConf(TestSuiteConfigs.smokeTestKafkaStreamESIndexKey) + "_" + cluster
  val smokeTestKafkaStreamBatchInterval: String = getConf(TestSuiteConfigs.smokeTestKafkaStreamBatchIntervalKey)
  val smokeTestStreamingAwaitTermination: String = getConf(TestSuiteConfigs.smokeTestStreamingAwaitTerminationKey)

  val defaultESCluster: String = props(GimelConstants.ES_POLLING_STORAGES)

  def hiveURL(cluster: String): String = {
    userProps.getOrElse(s"gimel.hive.$cluster.url", props(s"gimel.hive.$cluster.url"))
  }

  def esURL(escluster: String): String = {
    val alternateConfig = props(s"pcatalog.es.${defaultESCluster}.url")
    userProps.getOrElse(s"pcatalog.es.${escluster}.url", alternateConfig)
  }

  /**
    * Returns Properties from the resources file
    *
    * @return mutable.Map[String, String]
    */
  private def getProps: mutable.Map[String, String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val props: Properties = new Properties()
    val configStream = this.getClass.getResourceAsStream(GimelConstants.GIMEL_PROPERTIES_FILE_NAME)
    props.load(configStream)
    configStream.close()
    val finalProps: mutable.Map[String, String] = mutable.Map(props.asScala.toSeq: _*)
    logger.debug("PCatalog Properties -->")
    finalProps.foreach(property => logger.debug(property))
    finalProps
  }

  logger.info(s"Completed Building --> ${this.getClass.getName}")
}

/**
  * Companion Object
  */
object GimelTestSuiteProperties {

  /**
    * If nothing is supplied from User ; Load all props from file in resources folder
    *
    * @return PCatalogProperties
    */
  def apply(): GimelTestSuiteProperties = new GimelTestSuiteProperties()

  /**
    * Use the properties supplied by user & load the defaults from resources where-ever applicable
    *
    * @param params User Supplied properties as a KV Pair
    * @return PCatalogProperties
    */
  def apply(params: Map[String, String]): GimelTestSuiteProperties = new GimelTestSuiteProperties(params)

}

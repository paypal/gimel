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

package com.paypal.gimel.common.utilities.kafka

import java.util.{Collections, Properties}

import io.confluent.kafka.schemaregistry.storage.serialization.ZkStringSerializer
import kafka.admin.AdminUtils
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{CoreUtils, ZkUtils}
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.junit.rules.TemporaryFolder

import com.paypal.gimel.common.storageadmin.KafkaAdminUtils
import com.paypal.gimel.logger.Logger

/**
  * This logic is borrowed from confluent kafka-streams-examples.
  * https://github.com/confluentinc/kafka-streams-examples/blob/5.4.0-post/src/test/java/io/confluent/examples/streams/kafka/KafkaEmbedded.java
  *
  * Runs an in-memory, "embedded" instance of a Kafka broker, which listens at `127.0.0.1:9092` by
  * default.
  *
  * Requires a running ZooKeeper instance to connect to.  By default, it expects a ZooKeeper instance
  * running at `127.0.0.1:2181`.  You can specify a different ZooKeeper instance by setting the
  * `zookeeper.connect` parameter in the broker's configuration.
  */
class KafkaEmbedded(initialConfig: Properties) {

  private val log: Logger = Logger(this.getClass.getName)

  val DEFAULT_ZK_CONNECT = "127.0.0.1:2181"
  val DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000
  val DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000

  val tmpFolder = new TemporaryFolder()
  tmpFolder.create()
  val logDir = tmpFolder.newFolder()
  val effectiveConfig = effectiveConfigFrom(initialConfig)
  val loggingEnabled = true
  val kafkaConfig = new KafkaConfig(effectiveConfig, loggingEnabled)
  println(s"Starting embedded Kafka broker (with log.dirs=$logDir and ZK ensemble at ${zookeeperConnect()}) ...")
  val kafka = new KafkaServer(kafkaConfig)
  println(s"Startup of embedded Kafka broker at ${brokerList()} completed (with ZK ensemble at ${zookeeperConnect()}) ..." )

  def effectiveConfigFrom(initialConfig: Properties) : Properties = {
    val effectiveConfig = new Properties()
    effectiveConfig.put(KafkaConfig.BrokerIdProp, "0")
    effectiveConfig.put(KafkaConfig.HostNameProp, "127.0.0.1")
    effectiveConfig.put(KafkaConfig.PortProp, "9092")
    effectiveConfig.put(KafkaConfig.NumPartitionsProp, "1")
    effectiveConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, "true")
    effectiveConfig.put(KafkaConfig.MessageMaxBytesProp, "1000000")
    effectiveConfig.put(KafkaConfig.ControlledShutdownEnableProp, "true")
    effectiveConfig.put(KafkaConfig.ZkConnectProp, DEFAULT_ZK_CONNECT)

    effectiveConfig.putAll(initialConfig)
    effectiveConfig.setProperty(KafkaConfig.LogDirProp, logDir.getAbsolutePath())
    effectiveConfig
  }

  /**
    * This broker's `metadata.broker.list` value.  Example: `127.0.0.1:9092`.
    *
    * You can use this to tell Kafka producers and consumers how to connect to this instance.
    */
  def brokerList(): String = {
    String.join(":", kafka.config.hostName, effectiveConfig.getProperty(KafkaConfig.PortProp))
  }


  /**
    * The ZooKeeper connection string aka `zookeeper.connect`.
    */
  def zookeeperConnect(): String = {
    effectiveConfig.getProperty("zookeeper.connect", DEFAULT_ZK_CONNECT)
  }

  /**
    * Start the broker.
    */
  def start() {
    log.debug(s"Starting embedded Kafka broker at ${brokerList()} (with log.dirs=$logDir and ZK ensemble at ${zookeeperConnect()}) ...")
    kafka.startup()
    log.debug(s"Startup of embedded Kafka broker at ${brokerList()} completed (with ZK ensemble at ${zookeeperConnect()}) ...")
  }

  /**
    * Stop the broker.
    */
  def stop() {
    log.debug(s"Shutting down embedded Kafka broker at ${brokerList()} (with ZK ensemble at ${zookeeperConnect()}) ...")
    kafka.shutdown()
    kafka.awaitShutdown()
    log.debug(s"Removing logs.dir at $logDir ...")
    val logDirs = Collections.singletonList(logDir.getAbsolutePath())
    tmpFolder.delete();
    CoreUtils.delete(scala.collection.JavaConversions.asScalaBuffer(logDirs))
    log.debug(s"Shutdown of embedded Kafka broker at ${brokerList()} completed (with ZK ensemble at ${zookeeperConnect()}) ...")
  }

  /**
    * Create a Kafka topic with 1 partition and a replication factor of 1.
    *
    * @param topic The name of the topic.
    */
  def createTopic(topic: String) {
    createTopic(topic, 1, 1, new Properties())
  }

  /**
    * Create a Kafka topic with the given parameters.
    *
    * @param topic       The name of the topic.
    * @param partitions  The number of partitions for this topic.
    * @param replication The replication factor for (the partitions of) this topic.
    */
  def createTopic(topic: String, partitions: Int, replication: Int) {
    createTopic(topic, partitions, replication, new Properties())
  }

  /**
    * Create a Kafka topic with the given parameters.
    *
    * @param topic       The name of the topic.
    * @param partitions  The number of partitions for this topic.
    * @param replication The replication factor for (partitions of) this topic.
    * @param topicConfig Additional topic-level configuration settings.
    */
  def createTopic(topic: String,
    partitions: Int,
    replication: Int, topicConfig: Properties) {
    log.debug(
      s"""
         |Creating topic { name: $topic, partitions: $partitions, replication: $replication, config: $topicConfig }
         |""".stripMargin)
    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
    // createTopic() will only seem to work (it will return without error).  The topic will exist in
    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
    // topic.
    val zkClient = new ZkClient(
      zookeeperConnect(),
      DEFAULT_ZK_SESSION_TIMEOUT_MS,
      DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
      new ZkStringSerializer())
    val isSecure = false
    val zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperConnect()), isSecure)
    AdminUtils.createTopic(zkUtils, topic, partitions, replication, topicConfig)
    zkClient.close()
  }

  /**
    * Delete a Kafka topic with the given parameters.
    *
    * @param topic       The name of the topic.
    */
  def deleteTopicIfExists(topic: String): Unit = {
    KafkaAdminUtils.deleteTopicIfExists(zookeeperConnect(), topic)
  }
}

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

package com.paypal.gimel.serde.common.kafka

import java.util.Properties

import io.confluent.kafka.schemaregistry.RestApp
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel
import kafka.server.KafkaConfig
import org.apache.curator.test.InstanceSpec
import org.junit.rules.ExternalResource

import com.paypal.gimel.logger.Logger
import com.paypal.gimel.serde.common.zookeeper.ZooKeeperEmbedded

/*
 * This logic is borrowed from confluent kafka-streams-examples.
 * https://github.com/confluentinc/kafka-streams-examples/blob/5.4.0-post/src/test/java/io/confluent/examples/streams/kafka/EmbeddedSingleNodeKafkaCluster.java
 *
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
class EmbeddedSingleNodeKafkaCluster(val brokerConfig: Properties = new Properties()) extends ExternalResource {

  val logger = new Logger()
  val DEFAULT_BROKER_PORT = 9092 // 0 results in a random port being selected
  val DEFAULT_ZK_PORT = 2181
  val DEFAULT_SCHEMA_REGISTRY_PORT = 8081
  val KAFKA_SCHEMAS_TOPIC = "_schemas";
  val AVRO_COMPATIBILITY_TYPE = AvroCompatibilityLevel.NONE.name;

  var zookeeper: ZooKeeperEmbedded = _
  var broker: KafkaEmbedded = _
  var schemaRegistry: RestApp = _

  /**
    * Creates and starts a Kafka cluster.
    */
  def start() {
    logger.debug("Initiating embedded Kafka cluster startup")
    // val zkPort = InstanceSpec.getRandomPort()
    val zkPort = DEFAULT_ZK_PORT
    logger.debug("Starting a ZooKeeper instance on port " + zkPort)
    zookeeper = new ZooKeeperEmbedded(zkPort)
    zookeeper.start()
    logger.debug("ZooKeeper instance is running at " + zookeeper.connectString())

    val effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper)
    logger.debug("Starting a Kafka instance on port " +
      effectiveBrokerConfig.getProperty(KafkaConfig.PortProp))
    broker = new KafkaEmbedded(effectiveBrokerConfig)
    broker.start()
    logger.debug(s"Kafka instance is running at ${broker.brokerList()}, connected to ZooKeeper at ${broker.zookeeperConnect()}")

    schemaRegistry = new RestApp(
      InstanceSpec.getRandomPort(),
      zookeeperConnect(),
      KAFKA_SCHEMAS_TOPIC, AVRO_COMPATIBILITY_TYPE)
    schemaRegistry.start()
  }

  def effectiveBrokerConfigFrom(brokerConfig: Properties, zookeeper: ZooKeeperEmbedded): Properties = {
    val effectiveConfig = new Properties()
    effectiveConfig.put(KafkaConfig.ZkConnectProp, zookeeper.connectString())
    effectiveConfig.put(KafkaConfig.PortProp, DEFAULT_BROKER_PORT.toString)
    effectiveConfig.putAll(brokerConfig)
    effectiveConfig
  }


  override protected def before() {
    start();
  }

  override protected def after() {
    stop();
  }

  /**
    * Stop the Kafka cluster.
    */
  def stop() {
    try {
      schemaRegistry.stop();
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw new RuntimeException(ex)
      }
    }
    broker.stop();
    try {
      zookeeper.stop();
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw new RuntimeException(ex)
      }
    }
  }

  /**
    * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
    *
    * You can use this to tell Kafka producers how to connect to this cluster.
    */
  def bootstrapServers(): String = {
    broker.brokerList()
  }

  /**
    * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
    * Example: `127.0.0.1:2181`.
    *
    * You can use this to e.g. tell Kafka consumers how to connect to this cluster.
    */
  def zookeeperConnect(): String = {
    zookeeper.connectString()
  }

  /**
    * The "schema.registry.url" setting of this schema registry instance.
    */
  def schemaRegistryUrl(): String = {
    schemaRegistry.restConnect
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
                  replication: Int,
                  topicConfig: Properties) {
    broker.createTopic(topic, partitions, replication, topicConfig)
  }

}

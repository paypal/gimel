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

package com.paypal.gimel.common.storageadmin

import java.util.Properties

import kafka.admin._
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.{ZkClient, ZkConnection}
import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer
import org.apache.kafka.common.security.JaasUtils

import com.paypal.gimel.logger.Logger

object KafkaAdminUtils {

  val logger = Logger()

  val isSecurityEnabled = JaasUtils.isZkSecurityEnabled()
  val sessionTimeOutInMs: Int = 10 * 1000
  val connectionTimeOutInMs: Int = 10 * 1000
  val zkClient: (String) => ZkClient = new ZkClient(_: String, sessionTimeOutInMs, connectionTimeOutInMs, GimelZKStringSerializer)
  val zkConnection: (String) => ZkConnection = new ZkConnection(_: String, sessionTimeOutInMs)

  /**
    * Creates a Topic in Kafka if it does not exists
    *
    * @param zookKeeperHostAndPort Zookeeper Host & Port | Example localhost:2181
    * @param kafkaTopicName        Kafka Topic Name
    * @param numberOfPartitions    Number of Partitions
    * @param numberOfReplica       Number of Replicas
    */
  def createTopicIfNotExists(zookKeeperHostAndPort: String, kafkaTopicName: String, numberOfPartitions: Int, numberOfReplica: Int): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val topicName = kafkaTopicName
    val noOfPartitions = numberOfPartitions
    val noOfReplication = numberOfReplica
    val topicConfiguration = new Properties()
    val client = zkClient(zookKeeperHostAndPort)
    val connect = zkConnection(zookKeeperHostAndPort)
    val zkUtil: ZkUtils = new ZkUtils(client, connect, isSecurityEnabled)
    if (!AdminUtils.topicExists(zkUtil, topicName)) {
      AdminUtils.createTopic(zkUtil, topicName, noOfPartitions, noOfReplication, topicConfiguration)
      logger.info(AdminUtils.fetchTopicMetadataFromZk(kafkaTopicName, zkUtil))
    }
    connect.close()
  }

  /**
    * Delete a Topic if it exists
    *
    * @param zookKeeperHostAndPort Zookeeper Host & Port | Example localhost:2181
    * @param kafkaTopicName        Kafka Topic Name
    */
  def deleteTopicIfExists(zookKeeperHostAndPort: String, kafkaTopicName: String): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val topicName = kafkaTopicName
    val client = zkClient(zookKeeperHostAndPort)
    val connect = zkConnection(zookKeeperHostAndPort)
    val zkUtil: ZkUtils = new ZkUtils(client, connect, isSecurityEnabled)
    if (AdminUtils.topicExists(zkUtil, topicName)) {
      AdminUtils.deleteTopic(zkUtil, topicName)
    }
    connect.close()
  }

  /**
    * Check if a Given Topic exists
    *
    * @param zookKeeperHostAndPort Zookeeper Host & Port | Example localhost:2181
    * @param kafkaTopicName        Kafka Topic Name
    * @return True if Topic Exists , otherwise False
    */
  def isTopicExists(zookKeeperHostAndPort: String, kafkaTopicName: String): Boolean = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val client = zkClient(zookKeeperHostAndPort)
    val connect = zkConnection(zookKeeperHostAndPort)
    val zkUtil: ZkUtils = new ZkUtils(client, connect, isSecurityEnabled)
    val result = AdminUtils.topicExists(zkUtil, kafkaTopicName)
    connect.close()
    result
  }

}


object GimelZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data: Object): Array[Byte] = {
    data.asInstanceOf[String].getBytes("UTF-8")
  }

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null) {
      null
    }
    else {
      new String(bytes, "UTF-8")
    }
  }
}

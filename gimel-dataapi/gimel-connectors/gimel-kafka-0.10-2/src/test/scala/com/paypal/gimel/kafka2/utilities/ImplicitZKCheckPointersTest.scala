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

package com.paypal.gimel.kafka2.utilities

import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest._

import com.paypal.gimel.common.utilities.zookeeper.ZooKeeperEmbedded
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka2.utilities.ImplicitZKCheckPointers._
import com.paypal.gimel.logger.Logger

class ImplicitZKCheckPointersTest extends FunSpec with Matchers with BeforeAndAfterAll {

  val topic = "test_gimel"
  val DEFAULT_ZK_PORT = 2181
  var zookeeper: ZooKeeperEmbedded = _
  val checkpointRoot = "/pcatalog/kafka_consumer/checkpoint"
  val zkNode = checkpointRoot + "/test_cluster/test_user/test_app"
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true

  protected override def beforeAll(): Unit = {
    logger.debug("Starting a ZooKeeper instance on port " + DEFAULT_ZK_PORT)
    zookeeper = new ZooKeeperEmbedded(DEFAULT_ZK_PORT)
    zookeeper.start()
    logger.debug("ZooKeeper instance is running at " + zookeeper.connectString())
  }

  protected override def afterAll(): Unit = {
    zookeeper.stop()
  }

  describe("ZKCheckPointers") {
    it("should save the offset ranges at given zkNodes") {
      val offsetRanges = Array(OffsetRange(topic, 0, 100000, 300000), OffsetRange(topic, 1, 100000, 400000), OffsetRange(topic, 2, 100000, 500000))
      val zkHostAndNodes = ZooKeeperHostAndNodes(zookeeper.connectString(), Seq(zkNode + topic))
      val checkPointingInfo: (ZooKeeperHostAndNodes, Array[OffsetRange]) = (zkHostAndNodes, offsetRanges)
      checkPointingInfo.saveZkCheckPoint
      val offsetRangesFromZk: Array[OffsetRange] = zkHostAndNodes.fetchZkCheckPoint.get
      logger.info("OffsetRanges from Zookeeper -> " + offsetRangesFromZk.toStringOfKafkaOffsetRanges)
      assert(offsetRangesFromZk.sameElements(offsetRanges))
    }
  }

  describe("fetchZkCheckPoint") {
    it("should fetch the offset ranges from given zkNodes and zk hosts") {
      val offsetRanges = Array(OffsetRange(topic + "_2", 0, 100000, 300000), OffsetRange(topic + "_2", 1, 100000, 400000), OffsetRange(topic + "_2", 2, 100000, 500000))
      val zkHostAndNodes = ZooKeeperHostAndNodes(zookeeper.connectString(), Seq(zkNode + topic + "_2"))
      val checkPointingInfo: (ZooKeeperHostAndNodes, Array[OffsetRange]) = (zkHostAndNodes, offsetRanges)
      checkPointingInfo.saveZkCheckPoint
      val offsetRangesFromZk: Array[OffsetRange] = zkHostAndNodes.fetchZkCheckPoint.get
      logger.info("OffsetRanges from Zookeeper -> " + offsetRangesFromZk.toStringOfKafkaOffsetRanges)
      assert(offsetRangesFromZk.sameElements(offsetRanges))
    }
  }

  describe("deleteZkCheckPoint") {
    it("should delete the offset ranges from given zkNodes and zk hosts") {
      val zkHostAndNodes = ZooKeeperHostAndNodes(zookeeper.connectString(), Seq(zkNode))
      zkHostAndNodes.deleteZkCheckPoint()
      val offsetRanges = zkHostAndNodes.fetchZkCheckPoint
      assert(offsetRanges == None)
    }
  }
}

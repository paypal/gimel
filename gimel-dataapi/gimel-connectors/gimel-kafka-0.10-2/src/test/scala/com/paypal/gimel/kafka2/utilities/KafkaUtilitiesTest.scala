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
import org.scalatest.{BeforeAndAfterAll, FunSpec, Matchers}

import com.paypal.gimel.common.utilities.kafka.EmbeddedSingleNodeKafkaCluster
import com.paypal.gimel.kafka2.conf.KafkaConstants
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

class KafkaUtilitiesTest extends FunSpec with Matchers with BeforeAndAfterAll {
  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  val topic = "test_gimel_utilities"
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true
  val checkpointRoot = "/pcatalog/kafka_consumer/checkpoint"
  val zkNode = checkpointRoot + "/test_cluster/test_user/test_app/"
  val fetchRowsOnFirstRun = 100000

  protected override def beforeAll(): Unit = {
    kafkaCluster.start()
  }

  protected override def afterAll(): Unit = {
    kafkaCluster.deleteTopicIfExists(topic + "_10")
    kafkaCluster.stop()
  }

  describe("inStreamCheckPoint") {
    it("should checkpoint a given OffsetRange in zookeeper") {
      val offsetRanges = Array(OffsetRange(topic, 0, 100000, 300000), OffsetRange(topic, 1, 100000, 400000), OffsetRange(topic, 2, 100000, 500000))
      KafkaUtilities.inStreamCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic), offsetRanges)
      val lastCheckpoint = KafkaUtilities.getLastCheckPointFromZK(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic))
      logger.info("inStreamCheckPoint | lastCheckpoint => " + lastCheckpoint.get.toStringOfKafkaOffsetRanges)
      assert(lastCheckpoint.get.toStringOfKafkaOffsetRanges == offsetRanges.toStringOfKafkaOffsetRanges)
    }
  }

  describe("getNewOffsetRangeForReader") {
    it("should get the lastCheckpoint if exists from zookeeper") {
      // Mock offset ranges in zookeeper
      val offsetRanges = Array(OffsetRange(topic + "_2", 0, 100000, 300000), OffsetRange(topic + "_2", 1, 100000, 400000), OffsetRange(topic + "_2", 2, 100000, 500000))
      KafkaUtilities.inStreamCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_2"), offsetRanges)

      // Retrieve the last checkpoint
      val lastCheckpoint = Array(OffsetRange(topic + "_2", 0, 100000, 300000), OffsetRange(topic + "_2", 1, 100000, 400000), OffsetRange(topic + "_2", 2, 100000, 500000))
      val availableOffsetRanges = Array(OffsetRange(topic + "_2", 0, 300000, 400000), OffsetRange(topic + "_2", 1, 400000, 500000), OffsetRange(topic + "_2", 2, 500000, 600000))
      val lastCheckpointResult = KafkaUtilities.getNewOffsetRangeForReader(Some(lastCheckpoint), availableOffsetRanges, fetchRowsOnFirstRun)
      logger.info("getNewOffsetRangeForReader | lastCheckpointResult => " + lastCheckpointResult.toStringOfKafkaOffsetRanges)
      assert(lastCheckpointResult.toStringOfKafkaOffsetRanges == s"${topic}_2,0,300000,400000|${topic}_2,1,400000,500000|${topic}_2,2,500000,600000")
    }

    it("should get the available offset range and limit it using fetchRowsOnFirstRun if last checkpoint does not exist") {
      // Mock offset ranges in zookeeper
      val offsetRanges = Array(OffsetRange(topic + "_3", 0, 100000, 300000), OffsetRange(topic + "_3", 1, 100000, 400000), OffsetRange(topic + "_3", 2, 100000, 500000))
      KafkaUtilities.inStreamCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_3"), offsetRanges)

      // Retrieve the last checkpoint
      val lastCheckpoint = None
      val availableOffsetRanges = Array(OffsetRange(topic + "_3", 0, 300000, 400000), OffsetRange(topic + "_3", 1, 300000, 500000), OffsetRange(topic + "_3", 2, 300000, 600000))
      val resultOffsetRanges = KafkaUtilities.getNewOffsetRangeForReader(lastCheckpoint, availableOffsetRanges, fetchRowsOnFirstRun)
      logger.info("getNewOffsetRangeForReader | resultOffsetRanges -> " + resultOffsetRanges.toStringOfKafkaOffsetRanges)
      val resultShouldBe = Array(OffsetRange(topic + "_3", 0, 300000, 400000), OffsetRange(topic + "_3", 1, 400000, 500000), OffsetRange(topic + "_3", 2, 500000, 600000))
      assert(resultOffsetRanges.toStringOfKafkaOffsetRanges == resultShouldBe.toStringOfKafkaOffsetRanges)
    }
  }

  describe("clearCheckPoint") {
    it("should clear the lastCheckpoint if exists") {
      // Mock offset ranges in zookeeper
      val offsetRanges = Array(OffsetRange(topic + "_4", 0, 100000, 300000), OffsetRange(topic + "_4", 1, 100000, 400000), OffsetRange(topic + "_4", 2, 100000, 500000))
      KafkaUtilities.inStreamCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_4"), offsetRanges)

      // clear the last checkpoint
      KafkaUtilities.clearCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_4"), "")
      val lastCheckpoint = KafkaUtilities.getLastCheckPointFromZK(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_4"))
      assert(lastCheckpoint == None)
    }
  }

  describe("getLastCheckPointFromZK") {
    it("should get the lastCheckpoint if exists") {
      // Mock offset ranges in zookeeper
      val offsetRanges = Array(OffsetRange(topic + "_5", 0, 100000, 300000), OffsetRange(topic + "_5", 1, 100000, 400000), OffsetRange(topic + "_5", 2, 100000, 500000))
      KafkaUtilities.inStreamCheckPoint(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_5"), offsetRanges)
      val lastCheckpoint = KafkaUtilities.getLastCheckPointFromZK(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_5"))
      logger.info("getLastCheckPointFromZK | lastCheckpoint -> " + lastCheckpoint.get.toStringOfKafkaOffsetRanges)
      assert(lastCheckpoint.get.toStringOfKafkaOffsetRanges == offsetRanges.toStringOfKafkaOffsetRanges)
    }

    it("should return None if lastCheckpoint does not exist") {
      val lastCheckpoint = KafkaUtilities.getLastCheckPointFromZK(kafkaCluster.zookeeperConnect(), Seq(zkNode + topic + "_6"))
      assert(lastCheckpoint == None)
    }
  }

  describe("getCustomOffsetRangeForReader") {
    it("should get custom offset range if batch mode") {
      // Pass custom offset range to the method
      val customOffsetRangesBatch = KafkaUtilities.getCustomOffsetRangeForReader(Seq(topic + "_6"),
        s"""[{"topic": "${topic}_6","offsetRange": [{"partition": 0,"from": 33223688879,"to": 33223688889}]}]""",
        KafkaConstants.gimelAuditRunTypeBatch)
      logger.info("getCustomOffsetRangeForReader | customOffsetRanges -> " + customOffsetRangesBatch.toStringOfKafkaOffsetRanges)
      logger.info("customOffsetRangesBatch => " + customOffsetRangesBatch.toStringOfKafkaOffsetRanges)
      val resultOffsetRangeShouldBe = Array(OffsetRange(topic + "_6", 0, "33223688879".toLong, "33223688889".toLong))
      assert(customOffsetRangesBatch.toStringOfKafkaOffsetRanges == resultOffsetRangeShouldBe.toStringOfKafkaOffsetRanges)
    }

    it("should throw error if topic names do not match") {
      // Pass custom offset range to the method
      val exception = intercept[Exception] {
        KafkaUtilities.getCustomOffsetRangeForReader(Seq(topic + "_0"),
          s"""[{"topic": "${topic}_6","offsetRange": [{"partition": 0,"from": 33223688879,"to": 33223688889}]}]""",
          KafkaConstants.gimelAuditRunTypeBatch)
      }
      assert(exception.getMessage.contains(s"The topic specified in custom offset range does not match the subscribed topic!"))
    }

    it("should get custom offset range with -1 untilOffset if stream mode") {
      // Pass custom offset range to the method
      val customOffsetRangesStream = KafkaUtilities.getCustomOffsetRangeForReader(Seq(topic + "_7"),
        s"""[{"topic": "${topic}_7","offsetRange": [{"partition": 0,"from": 33223688879}]}]""",
        KafkaConstants.gimelAuditRunTypeStream)
      logger.info("getCustomOffsetRangeForReader | customOffsetRangesStream => " + customOffsetRangesStream.toStringOfKafkaOffsetRanges)
      val resultOffsetRangeShouldBe = Array(OffsetRange(topic + "_7", 0, "33223688879".toLong, "-1".toLong))
      assert(customOffsetRangesStream.toStringOfKafkaOffsetRanges == resultOffsetRangeShouldBe.toStringOfKafkaOffsetRanges)
    }
  }
}

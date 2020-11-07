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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.spark.SharedSparkSession
import com.paypal.gimel.kafka2.DataSet
import com.paypal.gimel.kafka2.conf.KafkaConfigs
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

class ImplicitKafkaConvertersTest extends FunSpec with Matchers with SharedSparkSession {

  val kafkaCluster = new EmbeddedKafkaCluster(1)
  val topic = "test_gimel_kafka_converter"
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true
  var dataSet: DataSet = _
  var appTag: String = _

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
    kafkaCluster.createTopic(topic)
    appTag = com.paypal.gimel.common.utilities.DataSetUtils.getAppTag(spark.sparkContext)
    dataSet = new DataSet(spark)
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    kafkaCluster.deleteTopic(topic)
  }

  describe("OffsetsConverter with Array[OffsetRange]") {
    it("should convert the array of offset ranges to string") {
      val offsetRangeArray: Array[OffsetRange] = Array(OffsetRange(topic, 0, 0, 100000),
        OffsetRange(topic, 1, 1, 100000),
        OffsetRange(topic, 2, 2, 100000))
      assert(offsetRangeArray.toStringOfKafkaOffsetRanges ==
        s"$topic,0,0,100000|$topic,1,1,100000|$topic,2,2,100000")
    }
  }

  describe("OffsetsConverter with OffsetRange") {
    it("should convert a single offset range to string") {
      val offsetRange = OffsetRange(topic, 0, 0, 100000)
      assert(offsetRange.toStringOfKafkaOffsetRange == s"$topic,0,0,100000")
    }
  }

  describe("CheckPointConverter") {
    it("should convert a checkpoint string in format topic|partition|fromOffset|untilOffset to OffsetRange") {
      val offsetRangeStr = CheckPointString(s"$topic,0,0,100000")
      assert(offsetRangeStr.toKafkaOffsetRange == OffsetRange(topic, 0, 0, 100000))
    }
  }

  describe("CheckPointsConverter") {
    it("should convert an array of checkpoint strings in format topic,partition,fromOffset,untilOffset " +
      "to array of OffsetRange") {
      val checkpointStringArray: Array[CheckPointString] = Array(CheckPointString(s"$topic,0,0,100000"),
        CheckPointString(s"$topic,1,1,100000"),
        CheckPointString(s"$topic,2,2,100000"))
      assert(checkpointStringArray.toKafkaOffsetRanges.sameElements(Array(OffsetRange(topic, 0, 0, 100000),
        OffsetRange(topic, 1, 1, 100000),
        OffsetRange(topic, 2, 2, 100000))))
    }
  }

  describe("TopicPartitionsConverter - toTopicAndPartitions") {
    it("should convert BrokersAndTopic to  Map[TopicAndPartition, (String, Int)]") {
      writeMockDataToTopic(topic)
      val brokerAndTopic = BrokersAndTopic(kafkaCluster.bootstrapServers(), topic)
      logger.info("TopicPartitionsConverter - toTopicAndPartitions | brokerAndTopic -> " + brokerAndTopic.toTopicAndPartitions)
      val brokerPort = kafkaCluster.bootstrapServers().split(":")
      assert(brokerAndTopic.toTopicAndPartitions.keys.head.topic == topic)
      assert(brokerAndTopic.toTopicAndPartitions.keys.head.partition() == 0)
      assert(brokerAndTopic.toTopicAndPartitions.values.head._1 == brokerPort(0))
      assert(brokerAndTopic.toTopicAndPartitions.values.head._2 == brokerPort(1).toInt)
    }

  }

  describe("TopicPartitionsConverter - toKafkaOffsetsPerPartition") {
    it("should convert BrokersAndTopic to an array of OffsetRanges") {
      val topicName = topic + "_1"
      writeMockDataToTopic(topicName)
      val brokerAndTopic = BrokersAndTopic(kafkaCluster.bootstrapServers(), topicName)
      val result = brokerAndTopic.toKafkaOffsetsPerPartition
      logger.info("TopicPartitionsConverter - toKafkaOffsetsPerPartition -> " + result.toStringOfKafkaOffsetRanges)
      assert(result.sameElements(Array(OffsetRange(topicName, 0, 0, 10))))
    }
  }

  describe("NewOffsetRangesProvider - toNewOffsetRanges") {
    it("should convert offsetRangePairs to New Range of Offsets based on left and right when leftSide is lower than rightSize") {
      val leftOffsetRange = Array(OffsetRange(topic, 0, 0, 100000), OffsetRange(topic, 1, 100, 100000), OffsetRange(topic, 2, 200, 100000))
      val rightOffsetRange = Array(OffsetRange(topic, 0, 100000, 200000), OffsetRange(topic, 1, 100000, 200000), OffsetRange(topic, 2, 100000, 200000))
      assert((leftOffsetRange, rightOffsetRange).toNewOffsetRanges.toStringOfKafkaOffsetRanges
        == s"$topic,0,100000,200000|$topic,1,100000,200000|$topic,2,100000,200000")
    }

    it("should throw an error when leftSide untilOffset is greater than rightSize untilOffset") {
      val leftOffsetRange = Array(OffsetRange(topic, 0, 0, 100000), OffsetRange(topic, 1, 100, 100000), OffsetRange(topic, 2, 200, 100000))
      val rightOffsetRange = Array(OffsetRange(topic, 0, 10000, 20000), OffsetRange(topic, 1, 100000, 200000), OffsetRange(topic, 2, 100000, 200000))
      val exception = intercept[Exception] {
        (leftOffsetRange, rightOffsetRange).toNewOffsetRanges
      }
      assert(exception.getMessage.contains(s"Left Side Until:Offset 100000 is Higher than Right Side Until:Offset 20000"))
    }

    it("should throw an error when leftSide fromOffset is greater than rightSize untilOffset") {
      val leftOffsetRange = Array(OffsetRange(topic, 0, 200000, 10000), OffsetRange(topic, 1, 100, 100000), OffsetRange(topic, 2, 200, 100000))
      val rightOffsetRange = Array(OffsetRange(topic, 0, 10000, 20000), OffsetRange(topic, 1, 100000, 200000), OffsetRange(topic, 2, 100000, 200000))
      val exception = intercept[Exception] {
        (leftOffsetRange, rightOffsetRange).toNewOffsetRanges
      }
      assert(exception.getMessage.contains(s"Left Side from:Offset 200000 is Already Beyond Right Side Until:Offset 20000"))
    }

    it("should throw an error when leftSide untilOffset is lower than rightSize fromOffset") {
      val leftOffsetRange = Array(OffsetRange(topic, 0, 0, 100000), OffsetRange(topic, 1, 100, 100000), OffsetRange(topic, 2, 200, 100000))
      val rightOffsetRange = Array(OffsetRange(topic, 0, 100100, 200000), OffsetRange(topic, 1, 100000, 200000), OffsetRange(topic, 2, 100000, 200000))
      val exception = intercept[Exception] {
        (leftOffsetRange, rightOffsetRange).toNewOffsetRanges
      }
      assert(exception.getMessage.contains(s"Left Side until:Offset 100000 is Lower Than Right Side from:Offset 100100. This usually indicates Data Loss !"))
    }
  }

  describe("OffsetRangeRestriction") {
    it("applyThresholdPerPartition should limit the OffsetRanges to the given threshold per partition") {
      val offsetRanges = Array(OffsetRange(topic, 0, 100000, 200000), OffsetRange(topic, 1, 100000, 300000), OffsetRange(topic, 2, 100000, 300000))
      assert(offsetRanges.applyThresholdPerPartition(100000).toStringOfKafkaOffsetRanges ==
        s"$topic,0,100000,200000|$topic,1,100000,200000|$topic,2,100000,200000")
    }

    it("parallelizeOffsetRanges should divide the OffsetRanges to the given minRowsPerParallel with given parallel factor") {
      val offsetRanges = Array(OffsetRange(topic, 0, 100000, 300000), OffsetRange(topic, 1, 100000, 400000), OffsetRange(topic, 2, 100000, 500000))
      assert(offsetRanges.parallelizeOffsetRanges(250, 100000).toStringOfKafkaOffsetRanges ==
        s"$topic,0,100000,200000|$topic,0,200000,300000|$topic,0,300000,300000|" +
          s"$topic,1,100000,200000|$topic,1,200000,300000|$topic,1,300000,400000|$topic,1,400000,400000|" +
          s"$topic,2,100000,200000|$topic,2,200000,300000|$topic,2,300000,400000|$topic,2,400000,500000|$topic,2,500000,500000")
    }
  }

  describe("TopicPropsConverter - toTopicAndPartitions") {
    it("should convert tuple of (Comma-Separated topics, Properties) into KafkaTopicAndPartitions") {
      // Mocking the data in topic
      writeMockDataToTopic(s"${topic}_2")
      writeMockDataToTopic(s"${topic}_3")
      val topicAndProps = TopicAndProps(s"${topic}_2,${topic}_3", Map(KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers()))
      val topicPartitions = topicAndProps.toTopicAndPartitions
      logger.info("TopicPropsConverter - toTopicAndPartitions -> " + topicPartitions)
      val brokerPort = kafkaCluster.bootstrapServers().split(":")
      assert(topicPartitions.keys
        .sameElements(Array(new TopicPartition(s"${topic}_2", 0),
          new TopicPartition(s"${topic}_3", 0))))
      assert(topicPartitions.get(new TopicPartition(s"${topic}_2", 0)).get._1 == brokerPort(0))
      assert(topicPartitions.get(new TopicPartition(s"${topic}_2", 0)).get._2 == brokerPort(1).toInt)
      assert(topicPartitions.get(new TopicPartition(s"${topic}_3", 0)).get._1 == brokerPort(0))
      assert(topicPartitions.get(new TopicPartition(s"${topic}_3", 0)).get._2 == brokerPort(1).toInt)
    }
  }

  describe("TopicPropsConverter - toKafkaOffsetsPerPartition") {
    it("should convert tuple of (Comma-Separated topics, Properties) into KafkaTopicAndPartitions") {
      // Mocking the data in topic
      writeMockDataToTopic(s"${topic}_4")
      writeMockDataToTopic(s"${topic}_5")
      val topicAndProps = TopicAndProps(s"${topic}_4,${topic}_5", Map(KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers()))
      val topicOffsetRanges = topicAndProps.toKafkaOffsetsPerPartition
      logger.info("TopicPropsConverter - toKafkaOffsetsPerPartition -> " + topicOffsetRanges.toStringOfKafkaOffsetRanges)
      assert(topicOffsetRanges.toStringOfKafkaOffsetRanges == s"test_gimel_kafka_converter_5,0,0,10|test_gimel_kafka_converter_4,0,0,10")
    }
  }

  def writeMockDataToTopic(topicName: String): Unit = {
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zKConnectString())
    val dataSetName = "Kafka.Local.default." + topicName
    val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> appTag)
    val dataFrame = mockDataInDataFrame(10)
    val serializedDF = dataFrame.toJSON.toDF
    serializedDF.show(1)
    val dfWrite = dataSet.write(dataSetName, serializedDF, datasetProps)
  }
}

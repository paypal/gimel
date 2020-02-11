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

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.kafka.EmbeddedSingleNodeKafkaCluster
import com.paypal.gimel.common.utilities.spark.SharedSparkSession
import com.paypal.gimel.kafka2.DataSet
import com.paypal.gimel.kafka2.conf.KafkaConfigs
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

class ImplicitKafkaConvertersTest extends FunSpec with SharedSparkSession {

  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
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
    kafkaCluster.deleteTopicIfExists(topic)
    kafkaCluster.stop()
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
      val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
        KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
        KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
      val dataSetName = "Kafka.Local.default." + topic
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val dataFrame = mockDataInDataFrame(10)
      val serializedDF = dataFrame.toJSON.toDF
      serializedDF.show(1)
      val dfWrite = dataSet.write(dataSetName, serializedDF, datasetProps)
      val brokerAndTopic = BrokersAndTopic(kafkaCluster.bootstrapServers(), topic)
      logger.info("TopicPartitionsConverter - toTopicAndPartitions | brokerAndTopic -> " + brokerAndTopic.toTopicAndPartitions)
      assert(brokerAndTopic.toTopicAndPartitions
        .sameElements(Map(TopicAndPartition(topic, 0) -> ("127.0.0.1", 9092))))
    }
  }

  describe("TopicPartitionsConverter - toKafkaOffsetsPerPartition") {
    it("should convert BrokersAndTopic to an array of OffsetRanges") {
      val brokerAndTopic = BrokersAndTopic(kafkaCluster.bootstrapServers(), topic)
      val result = brokerAndTopic.toKafkaOffsetsPerPartition
      logger.info("TopicPartitionsConverter - toKafkaOffsetsPerPartition -> " + result.toStringOfKafkaOffsetRanges)
      assert(result.sameElements(Array(OffsetRange(topic, 0, 0, 10))))
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
}

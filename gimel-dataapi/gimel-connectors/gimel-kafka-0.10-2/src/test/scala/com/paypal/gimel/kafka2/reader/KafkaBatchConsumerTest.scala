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

package com.paypal.gimel.kafka2.reader

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.kafka.EmbeddedSingleNodeKafkaCluster
import com.paypal.gimel.common.utilities.spark.SharedSparkSession
import com.paypal.gimel.kafka2.DataSet
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConfigs}
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

class KafkaBatchConsumerTest extends FunSpec with SharedSparkSession {
  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  var dataSet: DataSet = _
  val topic = "test_gimel_consumer"
  var appTag: String = _
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    kafkaCluster.start()
    kafkaCluster.createTopic(topic)
    kafkaCluster.createTopic(topic + "_1")
    appTag = com.paypal.gimel.common.utilities.DataSetUtils.getAppTag(spark.sparkContext)
    dataSet = new DataSet(spark)
  }

  protected override def afterAll(): Unit = {
    super.beforeAll()
    kafkaCluster.deleteTopicIfExists(topic)
    kafkaCluster.deleteTopicIfExists(topic + "_1")
    kafkaCluster.stop()
  }

  describe("consumeFromKakfa") {
    it ("should get the offset range for kafka partitions from zookeeper and connect to kafka to get source data as dataframe") {
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
      val (df, offsetRanges) = KafkaBatchConsumer.consumeFromKakfa(spark, new KafkaClientConfiguration(datasetProps))
      df.show
      // Deserialize json messages
      val deserializedDS: RDD[String] = df.rdd.map { eachRow => {
        eachRow.getAs("value").asInstanceOf[Array[Byte]].map(_.toChar).mkString
      } }
      val deserializedDF: DataFrame = spark.read.json(deserializedDS)
      logger.info("consumeFromKakfa | offsetRanges -> " + offsetRanges.toStringOfKafkaOffsetRanges)
      assert(deserializedDF.except(dataFrame).count() == 0)
      assert(offsetRanges.toStringOfKafkaOffsetRanges == s"$topic,0,0,10")
    }

    it ("should detect if a kafka topic is empty and return a dataframe with fields given as input") {
      val topicName = topic + "_empty"
      val fieldsBindToString = s"""[{"fieldName":"id","fieldType":"string","defaultValue":""},{"fieldName":"dob","fieldType":"date","defaultValue":"null"},{"fieldName":"age","fieldType":"int","defaultValue":"23"}]"""
      val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
        KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
        KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect(),
        GimelConstants.FIELDS_BIND_TO_JSON -> fieldsBindToString)
      val dataSetName = "Kafka.Local.default." + topicName
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag,
        KafkaConfigs.rowCountOnFirstRunKey -> "0")
      val dataFrame = mockDataInDataFrame(10)
      val serializedDF = dataFrame.toJSON.toDF
      serializedDF.show(1)
      val dfWrite = dataSet.write(dataSetName, serializedDF, datasetProps)
      val (df, offsetRanges) = KafkaBatchConsumer.consumeFromKakfa(spark, new KafkaClientConfiguration(datasetProps))
      df.show
      logger.info("consumeFromKakfa | Empty kafka topic | offsetRanges -> " + offsetRanges.toStringOfKafkaOffsetRanges)
      assert(df.count() == 0)
      assert(df.columns.sorted.sameElements(Array("id", "dob", "age").sorted))
      assert(offsetRanges.toStringOfKafkaOffsetRanges == s"$topicName,0,10,10")
    }
  }

  describe("getOffsetRange") {
    it ("should connect to zookeeper to get the last checkpoint if found otherwise gets the available offsets for each kafka partition") {
      val topicName = (topic + "_1")
      val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topicName,
        KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
        KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
      val dataSetName = "Kafka.Local.default." + topicName
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val dataFrame = mockDataInDataFrame(10)
      val serializedDF = dataFrame.toJSON.toDF
      serializedDF.show(1)
      val dfToWrite = dataSet.write(dataSetName, serializedDF, datasetProps)
      val (resultOffsetRanges, parallelizedOffsetRanges) = KafkaBatchConsumer.getOffsetRange(new KafkaClientConfiguration(datasetProps))
      logger.info("getOffsetRange | resultOffsetRanges -> (" +
        resultOffsetRanges.toStringOfKafkaOffsetRanges + ", " +
        parallelizedOffsetRanges.toStringOfKafkaOffsetRanges + ")")
      assert(resultOffsetRanges.toStringOfKafkaOffsetRanges == s"$topicName,0,0,10")
    }
  }
}

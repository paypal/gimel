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

package com.paypal.gimel.kafka2.writer

import org.scalatest.FunSuite

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.kafka2.{EmbeddedSingleNodeKafkaCluster, SharedSparkSession}
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConfigs}
import com.paypal.gimel.kafka2.reader.KafkaBatchConsumer
import com.paypal.gimel.logger.Logger

class KafkaBatchProducerTest extends FunSuite with SharedSparkSession {
  val kafkaCluster = new EmbeddedSingleNodeKafkaCluster()
  val topic = "test_gimel_producer"
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
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    kafkaCluster.deleteTopicIfExists(topic)
    kafkaCluster.deleteTopicIfExists(topic + "_1")
    kafkaCluster.stop()
  }

  test("produceToKafka - DataFrame") {
    val props : Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> (topic + "_1"),
      KafkaConfigs.kafkaServerKey -> kafkaCluster.bootstrapServers(),
      KafkaConfigs.zookeeperCheckpointHost -> kafkaCluster.zookeeperConnect())
    val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
    val datasetProps : Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
      GimelConstants.APP_TAG -> appTag)
    val dataFrame = mockDataInDataFrame(10)
    val serializedDF = dataFrame.toJSON.toDF
    serializedDF.collect
    val conf = new KafkaClientConfiguration(datasetProps)
    KafkaBatchProducer.produceToKafka(conf, serializedDF)
    val (df, offSetRanges) = KafkaBatchConsumer.consumeFromKakfa(spark, conf)
    assert(df.count() == 10)
  }
}

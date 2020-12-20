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

import scala.collection.immutable.Map

import org.scalatest._

import com.paypal.gimel.common.catalog.DataSetProperties
import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConfigs}

class KafkaOptionsLoaderUtilsTest extends FunSpec with Matchers {
  val topic = "test_gimel_consumer"

  describe("getAllKafkaTopicsOptionsFromLoader") {
    it("should get all options from kafka options loader") {
      val props: Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
        KafkaConfigs.kafkaOptionsLoaderKey -> "com.paypal.gimel.kafka2.utilities.MockKafkaoptionsLoader")
      val appTag = "test_app_tag"
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps: Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val conf = new KafkaClientConfiguration(datasetProps)
      assert(KafkaOptionsLoaderUtils.getAllKafkaTopicsOptionsFromLoader(conf)
        .sameElements(Map("test_gimel_consumer" -> Map("bootstrap.servers" -> "localhost:9092"))))
    }
  }

  describe("getAllKafkaTopicsOptions") {
    it("should get all options from kafka options loader if specified") {
      val props: Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
        KafkaConfigs.kafkaOptionsLoaderKey -> "com.paypal.gimel.kafka2.utilities.MockKafkaoptionsLoader")
      val appTag = "test_app_tag"
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps: Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val conf = new KafkaClientConfiguration(datasetProps)
      assert(KafkaOptionsLoaderUtils.getAllKafkaTopicsOptions(conf)
        .sameElements(Map("test_gimel_consumer" -> Map("bootstrap.servers" -> "localhost:9092"))))
    }

    it("should get default options if kafka options loader is not specified") {
      val props: Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
        KafkaConfigs.kafkaServerKey -> "localhost:9093")
      val appTag = "test_app_tag"
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps: Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val conf = new KafkaClientConfiguration(datasetProps)
      assert(KafkaOptionsLoaderUtils.getAllKafkaTopicsOptions(conf)
        .sameElements(Map("test_gimel_consumer" -> Map("bootstrap.servers" -> "localhost:9093"))))
    }
  }

  describe("getAllKafkaTopicsDefaultOptions") {
    it("should get all options from kafka options loader") {
      val props: Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> topic,
        KafkaConfigs.kafkaServerKey -> "localhost:9093")
      val appTag = "test_app_tag"
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps: Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val conf = new KafkaClientConfiguration(datasetProps)
      assert(KafkaOptionsLoaderUtils.getAllKafkaTopicsDefaultOptions(conf)
        .sameElements(Map("test_gimel_consumer" -> Map("bootstrap.servers" -> "localhost:9093"))))
    }
  }

  describe("getEachKafkaTopicToOptionsMap") {
    it("should get a map of each topic to its properties") {
      val props: Map[String, String] = Map(KafkaConfigs.whiteListTopicsKey -> s"$topic,${topic}_1,${topic}_2",
        KafkaConfigs.kafkaOptionsLoaderKey -> "com.paypal.gimel.kafka2.utilities.MockKafkaoptionsLoader")
      val appTag = "test_app_tag"
      val dataSetProperties = DataSetProperties("KAFKA", null, null, props)
      val datasetProps: Map[String, Any] = Map("dataSetProperties" -> dataSetProperties,
        GimelConstants.APP_TAG -> appTag)
      val conf = new KafkaClientConfiguration(datasetProps)
      val kafkaOptionsMap = KafkaOptionsLoaderUtils.getAllKafkaTopicsDefaultOptions(conf)
      assert(KafkaOptionsLoaderUtils.getEachKafkaTopicToOptionsMap(kafkaOptionsMap)
        == Map("test_gimel_consumer" -> Map("kafka.bootstrap.servers" -> "localhost:9092"),
          "test_gimel_consumer_1" -> Map("kafka.bootstrap.servers" -> "localhost:9092"),
          "test_gimel_consumer_2" -> Map("kafka.bootstrap.servers" -> "localhost:9092"))
      )
    }
  }
}

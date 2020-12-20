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
import scala.util.{Failure, Success, Try}

import com.paypal.gimel.common.conf.{GimelConstants, KafkaOptionsLoader}
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConfigs, KafkaConstants}
import com.paypal.gimel.kafka2.utilities.KafkaUtilities.logger

object KafkaOptionsLoaderUtils {

  /**
   * Get Kafka Options for all topics from the loader
   *
   * @param conf
   * @return Map of topic to kafka options retrieved from OptionsLoader
   */
  def getAllKafkaTopicsOptionsFromLoader(conf: KafkaClientConfiguration): Map[String, Map[String, String]] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val kafkaOptionsLoader =
      Class.forName(conf.kafkaOptionsLoader.get).newInstance.asInstanceOf[KafkaOptionsLoader]
    Try(
      kafkaOptionsLoader.loadKafkaOptions(conf.tableProps ++
        Map(GimelConstants.KAFKA_TOPICS_KEY -> conf.kafkaTopics))
    ) match {
      case Success(kafkaOptions) =>
        logger.info(s"Kafka Options loaded -> $kafkaOptions")
        kafkaOptions
      case Failure(exception) =>
        logger.error(s"Error while loading kafka options from ${conf.kafkaOptionsLoader.get} " +
          s"with the exception -> ${exception.getMessage}")
        throw exception
    }
  }

  /**
   * Get Kafka Options for all topics given in input
   *
   * @param conf
   * @return Map of topic to kafka options retrieved from OptionsLoader if specified
   */
  def getAllKafkaTopicsOptions(conf: KafkaClientConfiguration): Map[String, Map[String, String]] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    if (conf.kafkaOptionsLoader.isDefined) {
      val kafkaOptions = getAllKafkaTopicsOptionsFromLoader(conf)
      kafkaOptions.isEmpty match {
        case true =>
          getAllKafkaTopicsDefaultOptions(conf)
        case false =>
          kafkaOptions
      }
    } else {
      getAllKafkaTopicsDefaultOptions(conf)
    }
  }

  /**
   * Get Default Kafka Options for all topics given in input
   *
   * @param conf
   * @return Map of topics to kafka options
   */
  def getAllKafkaTopicsDefaultOptions(conf: KafkaClientConfiguration): Map[String, Map[String, String]] = {
    Map(conf.kafkaTopics -> Map(KafkaConfigs.kafkaServerKey -> conf.kafkaHostsAndPort))
  }

  /**
   * Get Kafka Options for the given topic in input
   *
   * @param kafkaOptions : Map of topic to kafka options retrieved from OptionsLoader if specified
   * @return Map of topic to kafka options retrieved from OptionsLoader if specified
   */
  def getEachKafkaTopicToOptionsMap(kafkaOptions: Map[String, Map[String, String]]): Map[String, Map[String, String]] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val kafkaOptionsWithKafkaKeyword: Map[String, Map[String, String]] = kafkaOptions.map{case (topicList, options) => {
      (topicList, options.map(eachOption => {
        if (!eachOption._1.startsWith(KafkaConstants.KAFKA_CONST)) {
          (s"${KafkaConstants.KAFKA_CONST}.${eachOption._1}", eachOption._2)
        } else {
          (eachOption._1, eachOption._2)
        }
      }))
    }}
    kafkaOptionsWithKafkaKeyword.flatMap(x => {
      x._1.split(",").map(each => (each, x._2))
    })
  }

}

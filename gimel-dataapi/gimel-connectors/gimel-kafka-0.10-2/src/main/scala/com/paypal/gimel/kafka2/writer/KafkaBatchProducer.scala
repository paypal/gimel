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

import java.util.Properties

import scala.collection.JavaConverters._
import scala.language.implicitConversions
import scala.reflect.runtime.universe._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConstants}
import com.paypal.gimel.kafka2.utilities.KafkaUtilitiesException

/**
  * Implements Produce to Kafka Logic Here
  */
object KafkaBatchProducer {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    * InTakes a DataFrame
    * Convert to Avro Record
    * Serialize the record into Bytes
    * Publish to Kafka
    *
    * @param conf KafkaClientConfiguration
    * @param data RDD
    */
  def produceToKafka[T: TypeTag](conf: KafkaClientConfiguration, data: RDD[T]): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val kafkaProps: Properties = conf.kafkaProducerProps
    val kafkaTopic = conf.kafkaTopics
    logger.info(s"Kafka Props for Producer -> ${kafkaProps.asScala.mkString("\n")}")
    logger.info("Begin Publishing to Kafka....")
    try {
      data.foreachPartition { eachPartition =>
        val producer: KafkaProducer[Nothing, T] = new KafkaProducer(kafkaProps)
        val resp = eachPartition.map { messageString =>
          val rec = new ProducerRecord(kafkaTopic, messageString)
          producer.send(rec)
        }
        resp.length
        producer.close()
      }
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        val msg =
          s"""
             |kafkaTopic -> ${kafkaTopic}
             |kafkaParams --> ${kafkaProps.asScala.mkString("\n")}}
          """.stripMargin
        throw new KafkaUtilitiesException(s"Failed While Pushing Data Into Kafka \n ${msg}")
      }
    }
    logger.info("Publish to Kafka - Completed !")
  }

  /**
    * InTakes a DataFrame
    * Serialize the record into Bytes
    * Publish to Kafka
    *
    * @param conf      KafkaClientConfiguration
    * @param dataFrame DataFrame
    */
  def produceToKafka(conf: KafkaClientConfiguration, dataFrame: DataFrame): Unit = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    val kafkaProps: Properties = conf.kafkaProducerProps
    val kafkaTopic = conf.kafkaTopics
    logger.info(s"Kafka Props for Producer -> ${kafkaProps.asScala.mkString("\n")}")
    logger.info("Begin Publishing to Kafka....")
    try {
      dataFrame
        .write
        .format(KafkaConstants.KAFKA_FORMAT)
        .option(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, conf.kafkaHostsAndPort)
        .option(KafkaConstants.KAFKA_TOPIC, kafkaTopic)
        .save()
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        val msg =
          s"""
             |kafkaTopic -> ${kafkaTopic}
             |kafkaParams --> ${kafkaProps.asScala.mkString("\n")}}
          """.stripMargin
        throw new KafkaUtilitiesException(s"Failed While Pushing Data Into Kafka \n ${msg}")
      }
    }
    logger.info("Publish to Kafka - Completed !")
  }
}

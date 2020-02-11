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

import scala.collection.immutable.Map
import scala.language.implicitConversions

import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.streaming.kafka010._
import spray.json._

import com.paypal.gimel.common.catalog.GimelCatalogJsonProtocol._
import com.paypal.gimel.datastreamfactory.{StructuredStreamingResult}
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConstants}
import com.paypal.gimel.kafka2.utilities.BrokersAndTopic
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka2.utilities.KafkaUtilities._

/**
  * Implements Kafka Stream Consumer Logic here
  */
object KafkaStreamConsumer {

  val logger = com.paypal.gimel.logger.Logger()


  /**
    *
    * Function to get the starting offsets for the stream to read from
    *
    * @param conf KafkaClientConfiguration object that holds the configuration paremeters
    * @param kafkaTopic The kafkaTopics list to subscribe to
    * @return Starting Offsets in a Map[TopicPartition, Long]
    */
  private def getStartOffsets(conf: KafkaClientConfiguration, kafkaTopic: String, brokers: String) = {
    if (conf.kafkaCustomOffsetRange.isEmpty()) {
      val lastCheckPoint: Option[Array[OffsetRange]] = getLastCheckPointFromZK(conf.zkHostAndPort, conf.zkCheckPoints)
      val availableOffsetRange: Array[OffsetRange] = BrokersAndTopic(brokers, kafkaTopic).toKafkaOffsetsPerPartition
      if (lastCheckPoint == None) {
        logger.info("No CheckPoint Found !")
        if(conf.kafkaAutoOffsetReset.equals(KafkaConstants.earliestOffset)) {
          logger.info("Fetching from the beginning")
          availableOffsetRange.map {
            x => (new TopicPartition(x.topic, x.partition) -> x.fromOffset)
          }.toMap
        }
        else {
          logger.info("Fetching from the latest offset")
          availableOffsetRange.map {
            x => (new TopicPartition(x.topic, x.partition) -> x.untilOffset)
          }.toMap
        }
      } else {
        logger.info(s"Found Checkpoint Value --> ${lastCheckPoint.get.mkString("|")}")
        lastCheckPoint.get.map {
          x => (new TopicPartition(x.topic, x.partition) -> x.untilOffset)
        }.toMap
      }
    }
    else {
      val customOffsetRangesForStream: Array[OffsetRange] = getCustomOffsetRangeForReader(conf.kafkaTopics.split(","), conf.kafkaCustomOffsetRange, KafkaConstants.gimelAuditRunTypeStream)
      customOffsetRangesForStream.map {
        x => (new TopicPartition(x.topic, x.partition) -> x.fromOffset)
      }.toMap
    }
  }

  /**
    *
    * Function to return the last saved checkpoint from zookeeper
    *
    * @param conf KafkaClientConfiguration object that holds the configuration paremeters
    * @return Optional checkpoint Offsets in a Array[OffsetRange]
    */
  private def getLastCheckPoint(conf: KafkaClientConfiguration) = {
    val lastCheckPoint: Option[Array[OffsetRange]] = getLastCheckPointFromZK(conf.zkHostAndPort, conf.zkCheckPoints)
    lastCheckPoint
  }

  /**
    *
    * Core Function to create a structured stream
    *
    * @param sparkSession the spark session passed by the user
    * @param conf KafkaClientConfiguration object that holds the configuration paremeters
    * @return StreamingResult in a StructuredStreamingResult Object
    */
  def createStructuredStream(sparkSession: SparkSession, conf: KafkaClientConfiguration, props: Map[String, Any]): StructuredStreamingResult = {
    try {
      val isStreamParallel = conf.isStreamParallel
      val streamParallels = conf.streamParallelismFactor
      logger.debug(
        s"""
           |isStreamParallel --> ${isStreamParallel}
           |streamParallels --> ${streamParallels}
      """.stripMargin)
      // Resolve all the Properties & Determine Kafka CheckPoint before reading from Kafka
      val (kafkaTopic, brokers) = ( conf.kafkaTopics, conf.kafkaHostsAndPort)
      logger.info(s"Zookeeper Server : ${conf.zkHostAndPort}")
      logger.info(s"Zookeeper Checkpoint : ${conf.zkCheckPoints}")
      val startOffsetsForStream: Map[TopicPartition, Long] = getStartOffsets(conf, kafkaTopic, brokers)
      val startOffsetsStructured = startOffsetsForStream.toList.groupBy(_._1.topic())
        .mapValues(_.map(x => (x._1.partition().toString, x._2)).toMap)
      val kafkaBootstrapServers = conf.kafkaHostsAndPort
      val topics = conf.kafkaTopics

      val dataStreamReader: DataStreamReader = sparkSession
        .readStream
        .format(KafkaConstants.KAFKA_FORMAT)
        .option(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapServers)
        .option(KafkaConstants.KAFKA_SUBSCRIBE, topics)
        .options(conf.kafkaConsumerProps)
        .option(KafkaConstants.KAFKA_START_OFFSETS, startOffsetsStructured.toJson.toString())
        .option(KafkaConstants.STREAM_FAIL_ON_DATA_LOSS, conf.failOnDataLoss)

      val kafkaDF = if (conf.maxRatePerTrigger.isEmpty) {
        dataStreamReader.load()
      } else {
        dataStreamReader
          .option(KafkaConstants.KAFKA_MAX_OFFSETS_PER_TRIGGER, conf.maxRatePerTrigger)
          .load()
      }

      val kafkaDFFiltered = if (conf.kafkaSourceFieldsList.toLowerCase == KafkaConstants.allKafkaSourceFields) {
        kafkaDF
      } else {
        val kafkaSourceFieldsList = conf.kafkaSourceFieldsList.split(",")
        kafkaDF.select(kafkaSourceFieldsList.map(name => col(name)): _*)
      }

      val kafkaDFParallelized = if (isStreamParallel) {
        kafkaDFFiltered.repartition(streamParallels).toDF(kafkaDFFiltered.columns: _*)
      } else {
        kafkaDFFiltered
      }

      // CheckPointer Function - CheckPoints each window
      val saveCheckPoint: Unit = inStructuredStreamCheckPoint(sparkSession, conf.zkHostAndPort, conf.zkCheckPoints)
      // Provide Option to Clear CheckPoint
      val deleteCheckPoint: (String) => Unit = clearCheckPoint(conf.zkHostAndPort, conf.zkCheckPoints, _: String)
      // Provide Option to Get DataFrame for a Simple String Message from Kafka Topic

      StructuredStreamingResult(kafkaDFParallelized, saveCheckPoint, deleteCheckPoint)
    } catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }
}


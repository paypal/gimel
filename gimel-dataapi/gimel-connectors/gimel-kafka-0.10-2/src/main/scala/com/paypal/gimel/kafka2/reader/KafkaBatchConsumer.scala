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

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.utilities.BindToFieldsUtils._
import com.paypal.gimel.kafka2.conf.{KafkaClientConfiguration, KafkaConstants}
import com.paypal.gimel.kafka2.utilities.{BrokersAndTopic, KafkaUtilitiesException}
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka2.utilities.KafkaUtilities._

/**
  * Implements Kafka Consumer Batch Here
  */
object KafkaBatchConsumer {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    * Connects to Kafka, and get the source data in dataframe
    *
    * @param sparkSession : SparkSession
    * @param conf           KafkaClientConfiguration
    * @return DataFrame
    * @return Read Till Array[OffsetRange]
    *
    */

  def consumeFromKakfa(sparkSession: SparkSession, conf: KafkaClientConfiguration): (DataFrame, Array[OffsetRange]) = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    val kafkaParams: Map[String, String] = conf.kafkaConsumerProps
    try {
      val (finalOffsetRanges, parallelizedRanges) = getOffsetRange(conf)

      val finalDF = if (isKafkaTopicEmpty(finalOffsetRanges) && !conf.fieldsBindToJSONString.isEmpty) {
        logger.info("Kafka Topic is Empty.")
        logger.info("Returning Datafame with fields in " + GimelConstants.FIELDS_BIND_TO_JSON)
        getEmptyDFBindToFields(sparkSession, conf.fieldsBindToJSONString)
      } else {
        val kafkaDF = parallelizedRanges.map(eachOffsetRange => sparkSession.read.format(KafkaConstants.KAFKA_FORMAT)
          .option(KafkaConstants.KAFKA_BOOTSTRAP_SERVERS, conf.kafkaHostsAndPort)
          .option("assign", s"""{"${eachOffsetRange.topic}":[${eachOffsetRange.partition}]}""")
          .option("startingOffsets", s"""{"${eachOffsetRange.topic}":{"${eachOffsetRange.partition}":${eachOffsetRange.fromOffset}}}""")
          .option("endingOffsets", s"""{"${eachOffsetRange.topic}":{"${eachOffsetRange.partition}":${eachOffsetRange.untilOffset}}}""")
          .load())
        val finalDF = if (conf.kafkaSourceFieldsList.toLowerCase == KafkaConstants.allKafkaSourceFields) {
          kafkaDF.reduce(_ union _)
        } else {
          val kafkaSourceFieldsList = conf.kafkaSourceFieldsList.split(",")
          kafkaDF.reduce(_ union _).select(kafkaSourceFieldsList.map(name => col(name)): _*)
        }
        finalDF
      }
      (finalDF, finalOffsetRanges)
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        val messageString =
          s"""
             |kafkaParams --> ${kafkaParams.mkString(" \n ")}
          """.stripMargin
        logger.error(s"An Error While Attempting to Consume From Kafka with Parameters -->  $messageString")
        throw ex
    }
  }

  /**
    * Connects to zookeeper to get the last checkpoint if found otherwise gets the available offsets for each kafka partition
    *
    * @param conf : KafkaClientConfiguration
    * @return (Array[OffsetRange], Array[OffsetRange]) : Tuple of available and parallelized offset ranges from zookeeper
    *
    */
  def getOffsetRange(conf: KafkaClientConfiguration): (Array[OffsetRange], Array[OffsetRange]) = {
    //  Read raw data from kafka topic
    val finalOffsetRangesForReader: Array[OffsetRange] = if (conf.kafkaCustomOffsetRange.isEmpty()) {
      logger.info(s"""No custom offset information was given by the user""")
      val lastCheckPoint: Option[Array[OffsetRange]] = getLastCheckPointFromZK(conf.zkHostAndPort, conf.zkCheckPoints)
      val availableOffsetRange: Array[OffsetRange] = BrokersAndTopic(conf.kafkaHostsAndPort, conf.kafkaTopics).toKafkaOffsetsPerPartition
      val newOffsetRangesForReader = getNewOffsetRangeForReader(lastCheckPoint, availableOffsetRange, conf.fetchRowsOnFirstRun)
      logger.info("Offset Ranges From Difference -->")
      newOffsetRangesForReader.foreach(x => logger.info(x.toString))
      newOffsetRangesForReader.applyThresholdPerPartition(conf.maxRecsPerPartition.toLong) // Restrict Offset Ranges By Applying Threshold Per Partition
    } else {
      logger.info(s"""Custom offset information was given by the user""")
      getCustomOffsetRangeForReader(conf.kafkaTopics.split(","), conf.kafkaCustomOffsetRange, KafkaConstants.gimelAuditRunTypeBatch)
    }
    logger.info("Offset Ranges After applying Threshold Per Partition/Custom Offsets -->")
    finalOffsetRangesForReader.foreach(x => logger.info(x.toString))
    val parallelizedRanges: Array[OffsetRange] = finalOffsetRangesForReader.parallelizeOffsetRanges(conf.parallelsPerPartition, conf.minRowsPerParallel)
    logger.info("Final Array of OffsetRanges to Fetch from Kafka --> ")
    parallelizedRanges.foreach(range => logger.info(range))
    if (parallelizedRanges.isEmpty) throw new KafkaUtilitiesException("There is an issue ! No Offset Range From Kafka ... Is the topic having any message at all ?")
    (finalOffsetRangesForReader, parallelizedRanges)
  }

  /**
    * Checks if the given kafka topics are empty
    *
    * @param offsetRanges : array of OffsetRanges for the topics to check
    * @return
    *
    */
  def isKafkaTopicEmpty(offsetRanges: Array[OffsetRange]): Boolean = {
    offsetRanges.isEmpty || offsetRanges.forall (each => (each.untilOffset - each.fromOffset) == 0)
  }
}

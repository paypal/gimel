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

package com.paypal.gimel.kafka.reader

import kafka.serializer._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.kafka010._
import scala.collection.immutable.Map
import scala.language.implicitConversions

import com.paypal.gimel.datastreamfactory.{CheckPointHolder, StreamingResult, WrappedData}
import com.paypal.gimel.kafka.avro.SparkAvroUtilities
import com.paypal.gimel.kafka.conf.KafkaClientConfiguration
import com.paypal.gimel.kafka.utilities.BrokersAndTopic
import com.paypal.gimel.kafka.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka.utilities.KafkaUtilities._

/**
  * Implements Kafka Stream Consumer Logic here
  */
object KafkaStreamConsumer {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    *
    * Core Function to Provide Data Stream
    *
    * @param streamingContext StreamingContext
    * @param conf             KafkaClientConfiguration
    * @return StreamingResult
    */
  def createDStream(streamingContext: StreamingContext, conf: KafkaClientConfiguration): StreamingResult = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val sparkConf = streamingContext.sparkContext.getConf
      val streamRate = sparkConf.get("throttle.streaming.maxRatePerPartition", conf.maxRatePerPartition)
      val isStreamParallel = sparkConf.get("throttle.streaming.isParallel", conf.isStreamParallel.toString).toBoolean
      val streamParallels = sparkConf.get("throttle.streaming.parallelism.factor", conf.streamParallelismFactor.toString).toInt
      logger.debug(
        s"""
           |isStreamParallel --> ${isStreamParallel}
           |streamParallels --> ${streamParallels}
      """.stripMargin)
      streamingContext.sparkContext.getConf
        .set("spark.streaming.backpressure.enabled", "true")
        .set("spark.streaming.kafka.maxRatePerPartition", streamRate)
      // Resolve all the Properties & Determine Kafka CheckPoint before reading from Kafka
      val (schemaString, kafkaTopic, brokers) = (conf.avroSchemaString, conf.kafkaTopics, conf.kafkaHostsAndPort)
      logger.info(s"Zookeeper Server : ${conf.zkHostAndPort}")
      logger.info(s"Zookeeper Checkpoint : ${conf.zkCheckPoints}")
      val startOffsetsForStream: Map[TopicPartition, Long] =
        if (conf.kafkaCustomOffsetRange.isEmpty()) {
          val lastCheckPoint: Option[Array[OffsetRange]] = getLastCheckPointFromZK(conf.zkHostAndPort, conf.zkCheckPoints)
          if (lastCheckPoint == None) {
            logger.info("No CheckPoint Found !")
          } else {
            logger.info(s"Found Checkpoint Value --> ${lastCheckPoint.get.mkString("|")}")
          }
          val availableOffsetRange: Array[OffsetRange] = BrokersAndTopic(brokers, kafkaTopic).toKafkaOffsetsPerPartition
           lastCheckPoint.getOrElse(availableOffsetRange).map {
            x => (new TopicPartition(x.topic, x.partition) -> x.untilOffset)
          }.toMap
        }
        else {
          val customOffsetRangesForStream: Array[OffsetRange] = getCustomOffsetRangeForReader(conf.kafkaCustomOffsetRange, conf.consumerModeStream)
          customOffsetRangesForStream.map {
            x => (new TopicPartition(x.topic, x.partition) -> x.fromOffset)
          }.toMap
        }
      var kafkaParams: Map[String, Object] = Map()
      conf.kafkaConsumerProps.foreach(x => kafkaParams += (x._1 -> x._2))
      val (keyDeSer, valDeSer) = (getSerDe(conf.kafkaKeyDeSerializer), getSerDe(conf.kafkaValueDeSerializer))
      kafkaParams +=("key.deserializer" -> keyDeSer, "value.deserializer" -> valDeSer)
      val consumerStrategy = ConsumerStrategies.Subscribe[Any, Any](kafkaTopic.split(",").toSet, kafkaParams, startOffsetsForStream)
      val locationStrategy = LocationStrategies.PreferConsistent
      logger.info(
        s"""
           |consumerStrategy --> ${consumerStrategy}
           |locationStrategy --> ${locationStrategy.toString}
           |Initiating createDirectStream with above Parameters...
        """.stripMargin)
      val msg: InputDStream[ConsumerRecord[Any, Any]] = KafkaUtils.createDirectStream(streamingContext, locationStrategy, consumerStrategy)
      var offsetRanges = Array[OffsetRange]()
      val messages1: DStream[WrappedData] = msg.transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //          CheckPointHolder().currentCheckPoint = offsetRanges
        CheckPointHolder().setCurentCheckPoint(offsetRanges)
        rdd
      }.map { x => WrappedData(x.key(), x.value()) }
      // CheckPointer Function - CheckPoints each window
      val saveCheckPoint: (Array[OffsetRange]) => Boolean = inStreamCheckPoint(conf.zkHostAndPort, conf.zkCheckPoints, _)
      // Convertor Function : takes Raw Data and Returns AvroGeneric Data
      val bytesToGenericRDD: (RDD[WrappedData]) => RDD[GenericRecord] =
        wrappedDataToAvro(_, conf.avroSchemaKey, conf.avroSchemaURL, conf.avroSchemaSource, conf.avroSchemaString, isStreamParallel, streamParallels, conf.cdhAllSchemaDetails)
      val finalSchema = conf.avroSchemaSource.toUpperCase() match {
        case "CDH" => addAdditionalFieldsToSchema(getAdditionalFields().keySet.toList, conf.cdhTopicSchemaMetadata.get)
        case _ => conf.avroSchemaString
      }
      // Convertor Function - RDD[GenericRecord] => DataFrame
      val genericRecToDF: (SQLContext, RDD[GenericRecord]) => DataFrame = SparkAvroUtilities.genericRecordtoDF(_, _, finalSchema)
      // Provide Option to Clear CheckPoint
      val deleteCheckPoint: (String) => Unit = clearCheckPoint(conf.zkHostAndPort, conf.zkCheckPoints, _: String)
      // Provide Option to Get DataFrame for a Simple String Message from Kafka Topic
      val columnAlias = kafkaMessageColumnAlias(conf)
      //      val wrappedDataToDF: (SQLContext, RDD[WrappedData]) => DataFrame = wrappedStringDataToDF(columnAlias, _, _)
      val wrappedDatatoDF1: (SQLContext, RDD[WrappedData]) => DataFrame = rddToDF(_, conf.kafkaMessageValueType, conf.kafkaKeySerializer, conf.kafkaValueSerializer, _, "value", conf.avroSchemaString, conf.avroSchemaSource, conf.cdhTopicSchemaMetadata, conf.cdhAllSchemaDetails)
      // Return a Wrapper of various functionalities to Client of this function
      StreamingResult(messages1, bytesToGenericRDD, genericRecToDF, wrappedDatatoDF1, saveCheckPoint, deleteCheckPoint)
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        streamingContext.stop()
        throw ex
      }
    }
  }

}


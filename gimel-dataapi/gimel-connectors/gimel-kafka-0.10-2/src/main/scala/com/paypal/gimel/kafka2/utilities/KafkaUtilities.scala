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

import java.io.Closeable

import scala.collection.immutable.Map
import scala.language.implicitConversions
import scala.reflect.runtime.universe._
import scala.util.parsing.json.JSON

import org.apache.kafka.common.serialization._
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.streaming.kafka010._
import spray.json._
import spray.json.DefaultJsonProtocol._

import com.paypal.gimel.datastreamfactory.StreamCheckPointHolder
import com.paypal.gimel.kafka2.conf._
import com.paypal.gimel.kafka2.conf.KafkaJsonProtocol.offsetPropertiesFormat
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.kafka2.utilities.ImplicitZKCheckPointers._


case class MessageInfo[T: TypeTag](key: String, message: T, topic: String, partition: Int, offset: Long)

/*
Case classes for reading custom offset properties from the user defined properties
 */
case class OffsetRangeProperties(partition: Int,
                                 from: Long,
                                 to: Option[Long])

case class OffsetProperties(topic: String,
                            offsetRange: Array[OffsetRangeProperties])

object KafkaUtilities {

  val logger = com.paypal.gimel.logger.Logger()

  /**
    * This is a Map of Properties that will be used to set the batch parameters
    * , based on the incoming volume of data & user supplied parameters
    */
  val defaultRowsPerBatch: Map[Int, Map[String, String]] = Map(
    100000000 -> Map(
      KafkaConfigs.batchFetchSize -> "500"
      , KafkaConfigs.maxRecordsPerPartition -> "100000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 50000000 -> Map(
      KafkaConfigs.batchFetchSize -> "500"
      , KafkaConfigs.maxRecordsPerPartition -> "50000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 25000000 -> Map(
      KafkaConfigs.batchFetchSize -> "250"
      , KafkaConfigs.maxRecordsPerPartition -> "25000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 10000000 -> Map(
      KafkaConfigs.batchFetchSize -> "100"
      , KafkaConfigs.maxRecordsPerPartition -> "10000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 1000000 -> Map(
      KafkaConfigs.batchFetchSize -> "20"
      , KafkaConfigs.maxRecordsPerPartition -> "1000000"
      , KafkaConfigs.minRowsPerParallelKey -> "100000"
    )
    , 100000 -> Map(
      KafkaConfigs.batchFetchSize -> "10"
      , KafkaConfigs.maxRecordsPerPartition -> "100000"
      , KafkaConfigs.minRowsPerParallelKey -> "10000"
    )
    , 30000 -> Map(
      KafkaConfigs.batchFetchSize -> "10"
      , KafkaConfigs.maxRecordsPerPartition -> "100000"
      , KafkaConfigs.minRowsPerParallelKey -> "10000"
    )
  )



  /**
    * Convenience Function to checkpoint a given OffsetRange
    *
    * @param zkHost      Host Server for Zookeeper
    * @param zkNodes     Node where we want to checkPoint
    * @param offsetRange Array[OffsetRange]
    * @return Boolean indicating checkpointing status
    */

  def inStreamCheckPoint(zkHost: String, zkNodes: Seq[String]
                         , offsetRange: Array[OffsetRange]): Boolean = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
    (zk, offsetRange).saveZkCheckPoint
  }

  /**
    * Convenience Function to checkpoint a given OffsetRange
    *
    * @param sparkSession Spark Session
    * @param zkHost       Host Server for Zookeeper
    * @param zkNodes      Node where we want to checkPoint
    * @return Boolean indicating checkpointing status
    */

  def inStructuredStreamCheckPoint(sparkSession: SparkSession, zkHost: String, zkNodes: Seq[String]): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)
    sparkSession.streams.addListener(new StreamingQueryListener() {
      override def onQueryStarted(event: QueryStartedEvent): Unit = Unit

      override def onQueryProgress(event: QueryProgressEvent): Unit = {
        val queryStatusMap = JSON.parseFull(event.progress.json).get.asInstanceOf[Map[String, Any]]
        val endOffsetsMap: Map[String, Map[Any, Any]] = queryStatusMap.get("sources").head.asInstanceOf[List[Any]]
          .head.asInstanceOf[Map[Any, Any]].get("endOffset").head.asInstanceOf[Map[String, Map[Any, Any]]]
        val endOffsets = endOffsetsMap.flatMap { x =>
          x._2.map { y =>
            OffsetRange(topic = x._1, partition = y._1.asInstanceOf[String].toInt, fromOffset = 0L, untilOffset = y._2.asInstanceOf[Double].longValue())
          }
        }.toArray
        StreamCheckPointHolder().setCurentCheckPoint(endOffsets)
        inStreamCheckPoint(zkHost, zkNodes, endOffsets)
      }

      override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
        sparkSession.streams.removeListener(this)
      }
    })
  }

  /**
    * Gets the Appropriate Serializer Class
    *
    * @param serializerClassName Name of the Serializer Class
    * @return Serializer Class
    */

  def getSerializer(serializerClassName: String)
  : Class[_ >: StringSerializer with ByteArraySerializer <: Serializer[_ >: String with Array[Byte]]] = {
    serializerClassName match {
      case "org.apache.kafka.common.serialization.StringSerializer" => {
        classOf[org.apache.kafka.common.serialization.StringSerializer]
      }
      case "org.apache.kafka.common.serialization.ByteArraySerializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArraySerializer]
      }
      case _ => {
        throw new Exception(s"UnSupported Serializer Class Requested : ${serializerClassName}")
      }
    }
  }

  /**
    * Gets the Appropriate DeSerializer Class
    *
    * @param deserializerClassName Name of the DeSerializer Class
    * @return DeSerializer Class
    */

  def getDeserializer(deserializerClassName: String)
  : Class[_ >: StringDeserializer with ByteArrayDeserializer <: Deserializer[_ >: String with Array[Byte]]] = {
    deserializerClassName match {
      case "org.apache.kafka.common.serialization.StringDeserializer" => {
        classOf[org.apache.kafka.common.serialization.StringDeserializer]
      }
      case "org.apache.kafka.common.serialization.ByteArrayDeserializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer]
      }
      case _ => {
        throw new Exception(s"UnSupported DeSerializer Class Requested : ${deserializerClassName}")
      }
    }
  }

  /**
    * Gets the Appropriate De/Serializer Class
    *
    * @param serDe Name of the De/Serializer Class
    * @return De/Serializer Class
    */

  def getSerDe(serDe: String): Class[_ >: StringDeserializer
    with ByteArrayDeserializer with StringSerializer with ByteArraySerializer <: Closeable] = {
    serDe match {
      case "org.apache.kafka.common.serialization.StringDeserializer" => {
        classOf[org.apache.kafka.common.serialization.StringDeserializer]
      }
      case "org.apache.kafka.common.serialization.ByteArrayDeserializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer]
      }
      case "org.apache.kafka.common.serialization.StringSerializer" => {
        classOf[org.apache.kafka.common.serialization.StringSerializer]
      }
      case "org.apache.kafka.common.serialization.ByteArraySerializer" => {
        classOf[org.apache.kafka.common.serialization.ByteArraySerializer]
      }
      case _ => {
        throw new Exception(s"UnSupported serDe Class Requested : ${serDe}")
      }
    }
  }


  /**
    * Completely Clear the CheckPointed Offsets, leading to Read from Earliest offsets from Kafka
    *
    * @param zkHost  Zookeeper Host
    * @param zkNodes Zookeeper Path
    * @param msg     Some Message or A Reason for Clearing CheckPoint
    */
  def clearCheckPoint(zkHost: String, zkNodes: Seq[String], msg: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
    zk.deleteZkCheckPoint()
  }


  /**
    * Gets the Latest CheckPoint from Zookeeper, if available
    *
    * @param zkHost  Host Server for Zookeeper
    * @param zkNodes Node where we want to checkPoint
    * @return Option[Array[OffsetRange]
    */

  def getLastCheckPointFromZK(zkHost: String, zkNodes: Seq[String]): Option[Array[OffsetRange]] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    try {
      val zk = ZooKeeperHostAndNodes(zkHost, zkNodes)
      val lastCheckPoint: Option[Array[OffsetRange]] = zk.fetchZkCheckPoint
      lastCheckPoint
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }

  /**
    * Function Gets
    * Either : The difference between lastCheckPoint & latestOffsetRange
    * Or : latestOffsetRange from Kafka
    *
    * @param lastCheckPoint       savedCheckPoint, if available
    * @param availableOffsetRange latestOfffsetRange from Kafka
    * @param fetchRowsOnFirstRun  This will be used if reading from kafka without
    *                             any prior checkpoint,
    *                             to ensure we read only last N messages
    *                             from topic as requested by client
    * @return Array[OffsetRange]
    */

  def getNewOffsetRangeForReader(lastCheckPoint: Option[Array[OffsetRange]]
                                 , availableOffsetRange: Array[OffsetRange]
                                 , fetchRowsOnFirstRun: Long): Array[OffsetRange] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    logger.info(" @Begin --> " + MethodName)

    try {
      val newOffsetRangesForReader = lastCheckPoint match {
        case None => {
          logger.warning(
            s"""No CheckPoint Found.
                |Reader will attempt to fetch "from beginning" From Kafka !""".stripMargin)
          availableOffsetRange.map {
            eachOffsetRange =>
              val fromOffset = scala.math.min(fetchRowsOnFirstRun
                , eachOffsetRange.untilOffset - eachOffsetRange.fromOffset)
              logger.info(s"Since this is first run," +
                s" will try to fetch only ${fromOffset} rows from Kafka")
              OffsetRange(eachOffsetRange.topic, eachOffsetRange.partition
                , eachOffsetRange.untilOffset - fromOffset, eachOffsetRange.untilOffset)
          }
        }
        case Some(lastCheckPoint) => {
          logger.info("""Found CheckPoint """)
          (lastCheckPoint, availableOffsetRange).toNewOffsetRanges
        }
      }
      newOffsetRangesForReader
    }
    catch {
      case ex: Throwable => {
        ex.printStackTrace()
        throw ex
      }
    }
  }

  /**
    * Function Gets
    * a custom offset range as a JSON from the user defined properties
    * Converts it to an array of offset ranges and returns them
    *
    * @param kafkaTopics sequence of topics
    * @param offsetRange user given custom offset ranges, if available
    * @return Array[OffsetRange]
    */

  def getCustomOffsetRangeForReader(kafkaTopics: Seq[String], offsetRange: String, consumerMode: String): Array[OffsetRange] = {
    def MethodName: String = new Exception().getStackTrace().apply(1).getMethodName()
    try {
      val offsetRangeObject = offsetRange.parseJson.convertTo[Seq[OffsetProperties]]
      val finalOffsetRanges = offsetRangeObject.flatMap {
        eachTopicRange =>
          eachTopicRange.offsetRange.map {
            eachOffsetRange => {
              var toOffset = 0L
              if (consumerMode == KafkaConstants.gimelAuditRunTypeStream) {
                toOffset = eachOffsetRange.to.getOrElse(-1)
              }
              else if (consumerMode == KafkaConstants.gimelAuditRunTypeBatch) {
                toOffset = eachOffsetRange.to.get
              }
              if (!kafkaTopics.contains(eachTopicRange.topic)) {
                throw new Exception("The topic specified in custom offset range does not match the subscribed topic! " +
                  "Please unset the previous value or check your properties")
              }
              OffsetRange(eachTopicRange.topic, eachOffsetRange.partition, eachOffsetRange.from, toOffset)
            }
          }
      }.toArray
      finalOffsetRanges
    } catch {
      case ex: Throwable =>
        ex.printStackTrace()
        throw ex
    }
  }
}

/**
  * Custom Exception for KafkaUtilities related errors
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
class KafkaUtilitiesException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}

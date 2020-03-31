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

package com.paypal.gimel.kafka.utilities

import java.{lang, util}
import java.util.{Collections, Properties}

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.logger.Logger

/**
  * Case Class to Represent a CheckPoint String. Example "flights,1,1,100"
  *
  * @param checkPoint
  */
case class CheckPointString(checkPoint: String)

/**
  * Case Class to Represent Brokers and Topics
  *
  * @param brokers Example : kafka_broker_ip:8081
  * @param topic   Example : flights
  */

case class BrokersAndTopic(brokers: String, topic: String)

/**
  * Provides a set of Implicit , Convenience APIs for developers to use
  */

object ImplicitKafkaConverters {

  val logger: Logger = Logger()

  /**
    * @param offsetRanges An Array of OffsetRange
    */
  implicit class OffsetsConverter(offsetRanges: Array[OffsetRange]) {

    /**
      * Converts An Array OffsetRange to String of [CheckPoints (comma-separated)], each checkpoint Separated by Pipe
      *
      * @example Array(OffsetRange("test", 0, 1, 100),OffsetRange("test", 1, 1, 100)).toStringOfKafkaOffsetRanges
      * @return String of [CheckPoints (comma-separated)], each checkpoint Separated by Pipe
      */
    def toStringOfKafkaOffsetRanges: String = {
      offsetRanges.map(offsetRange => offsetRange.toStringOfKafkaOffsetRange).mkString("|")
    }
  }


  /**
    * @param offsetRange A Kafka OffsetRange
    */
  implicit class OffsetConverter(offsetRange: OffsetRange) {
    /**
      * Converts a Kafka OffsetRange to A CheckPoint (comma-separated)
      *
      * @return A CheckPoint (comma-separated)
      * @example "test,0,0,4".toKafkaOffsetRanges
      */
    def toStringOfKafkaOffsetRange: String = {
      offsetRange.topic + "," + offsetRange.partition + "," + offsetRange.fromOffset + "," + offsetRange.untilOffset
    }
  }

  /**
    * @param checkPointString A CheckPoint (comma-separated)
    */
  implicit class CheckPointConverter(checkPointString: CheckPointString) {
    /**
      * Converts A CheckPoint (comma-separated) to An OffsetRange
      *
      * @return An OffsetRange
      * @example "test,0,0,4".toKafkaOffsetRanges
      */
    def toKafkaOffsetRange: OffsetRange = {
      val splitString = checkPointString.checkPoint.split(",")
      OffsetRange(splitString(0), splitString(1).toInt, splitString(2).toLong, splitString(3).toLong)
    }
  }

  /**
    * @param checkPointsString an Array of CheckPoints (comma-separated)
    */
  implicit class CheckPointsConverter(checkPointsString: Array[CheckPointString]) {
    /**
      * Converts an Array of CheckPoints (comma-separated) to An Array of OffsetRange
      *
      * @return An Array of OffsetRange
      * @example "test,0,0,4|test,1,0,5".split("|").toKafkaOffsetRanges
      */
    def toKafkaOffsetRanges: Array[OffsetRange] = {
      checkPointsString.map(eachOffsetString => eachOffsetString.toKafkaOffsetRange)
    }
  }


  /**
    * @param brokersAndTopic A Tuple of (Comma-Separated Hosts, TopicString)
    */
  implicit class TopicPartitionsConverter(brokersAndTopic: BrokersAndTopic) {

    val clientID: Int = scala.util.Random.nextLong().toInt
    val brokers: Array[String] = brokersAndTopic.brokers.split(",")
    val host1: String = brokers(0).split(":")(0)
    val port1: Int = brokers(0).split(":")(1).toInt
    val latestTime: Long = -1L
    val earliestTime: Long = -2L

    /**
      * Converts a given Tuple of KafkaBrokers & Topic into KafkaTopicAndPartitions
      *
      * @example val testing: Array[TopicAndPartition] = ("localhost:8080,localhost:8081", "test").toTopicAndPartitions
      * @return Array[TopicAndPartition]
      */
    def toTopicAndPartitions: Map[TopicPartition, (String, Int)] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val client = AdminClient.create(KafkaUtilities.getDefaultConsumerPropertiesPerBroker(brokersAndTopic.brokers))
      import scala.collection.JavaConverters._
      try {
        client.describeTopics(
          Collections.singletonList(brokersAndTopic.topic)
        ).all().get().asScala.flatMap { topicMetadata => {
          topicMetadata._2.partitions().asScala.map {
            partitionMetadata =>
              partitionMetadata.isr()
              (new TopicPartition(topicMetadata._1, partitionMetadata.partition()),
                (partitionMetadata.leader().host(), partitionMetadata.leader().port()))
          }
        }
        }.toMap
      } finally {
        client.close()
      }

    }

    /**
      * Converts a given Tuple of KafkaBrokers & Topic into Array[OffsetRange] available currently in Kafka Cluster
      *
      * @example val kafkaOffsets:Array[OffsetRange] = ("localhost:8080,localhost:8081", "test").toKafkaOffsetsPerPartition
      * @return Array[OffsetRange]
      *
      */
    def toKafkaOffsetsPerPartition: Array[OffsetRange] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName
      logger.info(" @Begin --> " + MethodName)

      val topicAndPartitions: Map[TopicPartition, (String, Int)] = brokersAndTopic.toTopicAndPartitions
      import scala.collection.JavaConverters._
      val partitions = topicAndPartitions.keySet.asJava

      logger.info("The Topic And Partitions are --> ")
      topicAndPartitions.foreach(println)

      val kafkaConsumer = KafkaUtilities.getKafkaConsumer(Some(
        KafkaUtilities.getDefaultConsumerPropertiesPerBroker(brokersAndTopic.brokers)
      ))
      try {
        val beginningOffsets: util.Map[TopicPartition, lang.Long] = kafkaConsumer.beginningOffsets(partitions)
        val endOffsets: util.Map[TopicPartition, lang.Long] = kafkaConsumer.endOffsets(partitions)
        topicAndPartitions.map {
          topicAndPartition =>
            OffsetRange(topicAndPartition._1.topic, topicAndPartition._1.partition,
              beginningOffsets.get(topicAndPartition._1), endOffsets.get(topicAndPartition._1))
        }.toArray
      } finally {
        kafkaConsumer.close()
      }
    }

    /**
      * Take a TopicAndPartition and Returns a Tuple of leader Host & Port
      *
      * @param topicAndPartition Kafka TopicAndPartition
      * @return Tuple(host, port)
      */
    private def findLeader(topicAndPartition: (TopicPartition, (String, Int))): (String, Int) = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)
      val leaderDetails: (String, Int) = (topicAndPartition._2._1, topicAndPartition._2._2)
      leaderDetails
    }
  }


  /**
    * @param offsetRangePairs an Array of Tuple(OffsetRange, OffsetRange). LeftSide Should be Lower Than RightSize
    */
  implicit class NewOffsetRangesProvider(offsetRangePairs: (Array[OffsetRange], Array[OffsetRange])) {
    /**
      * Calculates the New Range of Offsets to Read from Kafka based on a Pair of OffsetRange
      *
      * @return Array[OffsetRange]
      * @example (Array(OffsetRange("a", 0, 1, 1), OffsetRange("a", 1, 2, 100)) ,Array( OffsetRange("a", 1, 2, 100),OffsetRange("a", 0, 1, 100))).toNewOffsetRange
      */
    def toNewOffsetRanges: Array[OffsetRange] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val sortedLeft = offsetRangePairs._1.sortBy(offsetRange => offsetRange.partition)
      val sortedRight = offsetRangePairs._2.sortBy(offsetRange => offsetRange.partition)
      val combinedAfterSort = sortedLeft.zip(sortedRight)
      combinedAfterSort.map { eachPair =>
        val left = eachPair._1
        val right = eachPair._2
        if (left.topic != right.topic) throw new KafkaOperationsException(s"Invalid Operation ! Seems we are comparing two different topics --> ${left.topic} <> ${right.topic} ")
        if (left.untilOffset > right.untilOffset) throw new KafkaOperationsException(s"Left Side Until:Offset ${left.untilOffset} is Higher than Right Side Until:Offset ${right.untilOffset}")
        if (left.fromOffset > right.untilOffset) throw new KafkaOperationsException(s"Left Side from:Offset ${left.fromOffset} is Already Beyond Right Side Until:Offset ${right.untilOffset}")
        if (left.untilOffset < right.fromOffset) throw new KafkaOperationsException(s"Left Side from:Offset ${left.untilOffset} is Lower Than Right Side from:Offset ${right.untilOffset}. This usually indicates Data Loss !")
        val fromOffset = {
          if (left.untilOffset == right.untilOffset) {
            right.untilOffset
          } else {
            left.untilOffset
          }
        }
        OffsetRange(left.topic, left.partition, fromOffset, right.untilOffset)
      }
    }
  }

  /**
    * @param offsetRanges An Array of OffsetRange
    */
  implicit class OffsetRangeRestriction(offsetRanges: Array[OffsetRange]) {
    /**
      * Limits the OffsetRanges to the given threshold per partition
      *
      * @example val kafkaOffsets:Array[OffsetRange] = Array(OffsetRange(("localhost:8080,localhost:8081", "test"))).applyThresholdPerPartition(100)
      * @return Array[OffsetRange]
      *
      */
    def applyThresholdPerPartition(maxPerPartition: Long): Array[OffsetRange] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      offsetRanges.map {
        eachOffsetRange =>
          val fromOffset = eachOffsetRange.fromOffset
          val maxUntil = fromOffset + maxPerPartition
          val untilOffset = eachOffsetRange.untilOffset
          val newUntilOffset = scala.math.min(untilOffset, maxUntil)
          OffsetRange(eachOffsetRange.topic, eachOffsetRange.partition, eachOffsetRange.fromOffset, newUntilOffset)
      }
    }

    /**
      * Parallelizes an Array of Offset Range, by applying parallelism factor on each Offset Range
      *
      * @param parallelism Number of parallel shards
      * @return Array[OffsetRange]
      */
    def parallelizeOffsetRanges(parallelism: Int, minRowsPerParallel: Long): Array[OffsetRange] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val returningRanges = offsetRanges.flatMap(erange => parallelizeOffsetRange(erange, parallelism, minRowsPerParallel))
      logger.info("Outgoing Array of OffsetRanges --> ")
      returningRanges.foreach(logger.info(_))
      returningRanges
    }

    // parallelizeOffsetRange(OffsetRange("a", 1, 1, 20), 3)
    private def parallelizeOffsetRange(eachRange: OffsetRange, parallel: Int, minRowsPerParallel: Long): Array[OffsetRange] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val total = eachRange.untilOffset - eachRange.fromOffset
      if ((total > minRowsPerParallel)) {
        logger.info(s"Incoming Range --> $eachRange")
        logger.info(s"Parallel Factor --> $parallel")
        val returningRange: scala.collection.mutable.ArrayBuffer[OffsetRange] = ArrayBuffer()

        val recordsPer = scala.math.max(total / parallel, minRowsPerParallel)
        var cntr = eachRange.fromOffset
        val end = eachRange.untilOffset
        while (cntr < end) {
          returningRange.append(OffsetRange(eachRange.topic, eachRange.partition, cntr, cntr + recordsPer))
          cntr = cntr + recordsPer
          if (cntr + recordsPer > end) {
            returningRange.append(OffsetRange(eachRange.topic, eachRange.partition, cntr, end))
            cntr = end
          }
        }
        logger.info("Parallelized Ranges for the given OffsetRange ..")
        returningRange.foreach(logger.info(_))
        returningRange.toArray
      } else {
        logger.info(s"Not Applying Parallelism as the total rows : $total in this Offset Range < min rows per parallel : $minRowsPerParallel ")
        Array(eachRange)
      }
    }
  }

}

/**
  * Custom Exception
  *
  * @param message Message to Throw
  * @param cause   A Throwable Cause
  */
private class KafkaOperationsException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}

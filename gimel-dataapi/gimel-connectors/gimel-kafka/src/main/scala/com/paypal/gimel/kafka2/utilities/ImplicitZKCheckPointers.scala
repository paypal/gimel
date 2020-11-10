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

import scala.language.implicitConversions

import org.apache.spark.streaming.kafka010.OffsetRange

import com.paypal.gimel.common.storageadmin.ZooKeeperAdminClient._
import com.paypal.gimel.kafka2.utilities.ImplicitKafkaConverters._
import com.paypal.gimel.logger.Logger

case class ZooKeeperHostAndNodes(host: String, nodes: Seq[String])

/**
  * Provides Implicit, Convenience Functions for Developers to Do CheckPointing Operations
  */
object ImplicitZKCheckPointers {

  val logger = Logger()

  /**
    * @param checkPointingInfo Tuple of (ZooKeeperHostAndNode, Array[Kafka OffsetRange])
    */
  implicit class ZKCheckPointers(checkPointingInfo: (ZooKeeperHostAndNodes, Array[OffsetRange])) {
    /**
      * CheckPoints a Tuple of (Array[OffsetRange], checkPointDirectory)
      *
      * @example (Array(OffsetRange("l1", 11, 1, 1)),"/Users/dmohanakumarchan/Downloads/checkpoint/").saveCheckPoint
      * @return true if Success
      *
      */
    def saveZkCheckPoint: Boolean = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val zkServers = checkPointingInfo._1.host
      val zkNodes = checkPointingInfo._1.nodes
      val contentToWrite = checkPointingInfo._2.toStringOfKafkaOffsetRanges
      try {
        zkNodes.map { zkNode =>
          writetoZK(zkServers, zkNode, contentToWrite)
        }
      } catch {
        case ex: Throwable =>
          throw ex
      }
      true
    }

  }


  /**
    * @param zooKeeperDetails ZooKeeperHostAndNode
    */
  implicit class ZKCheckPointFetcher(zooKeeperDetails: ZooKeeperHostAndNodes) {
    /**
      * Fetches CheckPoints as An Array[OffsetRange]
      *
      * @example ("/Users/dmohanakumarchan/Downloads/checkpoint").fetchCheckPoint
      * @return Some(Array[OffsetRange])
      *
      */
    def fetchZkCheckPoint: Option[Array[OffsetRange]] = {
      def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

      logger.info(" @Begin --> " + MethodName)

      val zkServers = zooKeeperDetails.host
      val zkNodes = zooKeeperDetails.nodes
      if (zkServers.isEmpty) throw new ZooKeeperCheckPointerException("Expected CheckPoint Directory, but got Empty String !")
      val zkCheckPoints = zkNodes.flatMap { zkNode =>
        val checkPointString: Option[String] = readFromZK(zkServers, zkNode)
        checkPointString match {
          case None =>
            None
          case _: Option[String] =>
            checkPointString.get.split('|').map(x => CheckPointString(x)).toKafkaOffsetRanges
        }
      }.filter {
        None => true
      }.toArray
      if (zkCheckPoints.isEmpty) {
        None
      }
      else {
        Some(zkCheckPoints)
      }
    }

    /**
      * Deletes a ZooKeeper CheckPoint
      */
    def deleteZkCheckPoint(): Unit = {
      logger.warning(s"WARNING !!!!! Deleting --> host : ${zooKeeperDetails.host} | node : ${zooKeeperDetails.nodes}")
      try {
        zooKeeperDetails.nodes.map { node =>
          deleteNodeOnZK(zooKeeperDetails.host, node)
        }
      } catch {
        case ex: Throwable =>
          throw ex
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
private class ZooKeeperCheckPointerException(message: String, cause: Throwable)
  extends RuntimeException(message) {
  if (cause != null) {
    initCause(cause)
  }

  def this(message: String) = this(message, null)
}

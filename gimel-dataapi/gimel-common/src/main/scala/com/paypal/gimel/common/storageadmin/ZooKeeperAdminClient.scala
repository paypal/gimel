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

package com.paypal.gimel.common.storageadmin

import com.twitter.zookeeper.ZooKeeperClient

import com.paypal.gimel.logger.Logger

/**
  * Provides HDFS Read And Write operations
  */

object ZooKeeperAdminClient {

  val logger = Logger()

  /**
    * Writes a String Content to ZK Node
    *
    * @param zNode    Fully Qualified Path of the File to Write data into
    * @param someData Content to Write
    */
  def writetoZK(zServers: String, zNode: String, someData: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper WRITE Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    // try-with-resources block dint work here as ZooKeeperClient does not implements AutoClosable
    var zookeeperClient: ZooKeeperClient = null
    try {
      zookeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        logger.warning(s"Creating Zookeeper Node --> $zNode in Host --> $zServers ")
        zookeeperClient.createPath(zNode)
      }
      val bytesToWrite = someData.getBytes
      zookeeperClient.set(zNode, bytesToWrite)
      logger.info(s"Persisted in Node -> $zNode in Value --> $someData")
    } catch {
      case ex: Throwable =>
        throw ex
    } finally {
      if (zookeeperClient != null) {
        zookeeperClient.close()
      }
    }
  }

  /**
    * Reads the Contents of a ZK Node
    *
    * @param zNode Fully Qualified Path of the File to Read
    * @return Content of the File
    */
  def readFromZK(zServers: String, zNode: String): Option[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper READ Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    var read: Option[String] = None
    // try-with-resources block dint work here as ZooKeeperClient does not implements AutoClosable
    var zookeeperClient: ZooKeeperClient = null
    try {
      zookeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        logger.warning(s"Path does not exists --> $zNode ! Will return None.")
      } else {
        read = Some(new String(zookeeperClient.get(zNode)))
        logger.info(s"Found value --> $read")
      }
      read
    } catch {
      case ex: Throwable =>
        throw ex
    } finally {
      if (zookeeperClient != null) {
        zookeeperClient.close()
      }
    }
  }

  /**
    * Delete a Node on ZK
    *
    * @param zNode Fully Qualified Path of the File to Write data into
    */
  def deleteNodeOnZK(zServers: String, zNode: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper DELETE Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    // try-with-resources block dint work here as ZooKeeperClient does not implements AutoClosable
    var zookeeperClient: ZooKeeperClient = null
    try {
      zookeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        logger.warning(s"Path does not exists --> $zNode ! Will delete None.")
        None
      } else {
        zookeeperClient.delete(zNode)
        logger.warning(s"Deleted Node -> $zNode")
      }
    } catch {
      case ex: Throwable =>
        throw ex
    } finally {
      if (zookeeperClient != null) {
        zookeeperClient.close()
      }
    }
  }
}

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

object ZooKeeperAdminClient extends Logger {

  /**
    * Writes a String Content to ZK Node
    *
    * @param zNode    Fully Qualified Path of the File to Write data into
    * @param someData Content to Write
    */
  def writetoZK(zServers: String, zNode: String, someData: String): Unit = withMethdNameLogging { methodName =>
    info(s"Zookeeper WRITE Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    try {
      val zookeeperClient: ZooKeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        warning(s"Creating Zookeeper Node --> $zNode in Host --> $zServers ")
        zookeeperClient.createPath(zNode)
      }
      val bytesToWrite = someData.getBytes
      zookeeperClient.set(zNode, bytesToWrite)
      info(s"Persisted in Node -> $zNode in Value --> $someData")
      zookeeperClient.close()
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
    * Reads the Contents of a ZK Node
    *
    * @param zNode Fully Qualified Path of the File to Read
    * @return Content of the File
    */
  def readFromZK(zServers: String, zNode: String): Option[String] = withMethdNameLogging { methodName =>
    info(s"Zookeeper READ Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    var read: Option[String] = None
    try {
      val zookeeperClient: ZooKeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        warning(s"Path does not exists --> $zNode ! Will return None.")
      } else {
        read = Some(new String(zookeeperClient.get(zNode)))
        info(s"Found value --> $read")
      }
      zookeeperClient.close()
      read
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
    * Delete a Node on ZK
    *
    * @param zNode Fully Qualified Path of the File to Write data into
    */
  def deleteNodeOnZK(zServers: String, zNode: String): Unit = withMethdNameLogging { methodName =>

    info(s"Zookeeper DELETE Request for --> \nzServers --> $zServers \nzNode --> $zNode \n")
    try {
      val zookeeperClient: ZooKeeperClient = new ZooKeeperClient(zServers)
      if (zookeeperClient.exists(zNode) == null) {
        warning(s"Path does not exists --> $zNode ! Will delete None.")
        None
      } else {
        zookeeperClient.delete(zNode)
        warning(s"Deleted Node -> $zNode")
      }
      zookeeperClient.close()
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

}

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

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry

import com.paypal.gimel.common.utilities.GenericUtils
import com.paypal.gimel.logger.Logger

/**
  * Provides HDFS Read And Write operations
  */

object ZooKeeperAdminClient {

  val logger = Logger()

  /**
    * Writes a String Content to ZK Node
    *
    * @param zkServers : Zookeeper hosts and port
    * @param zNode :   Fully Qualified Path of the File to Write data into
    * @param someData : Content to Write
    */
  def writetoZK(zkServers: String, zNode: String, someData: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper WRITE Request for --> \nzServers --> $zkServers \nzNode --> $zNode \n")
    try {
      GenericUtils.withResources(getZKConnection(zkServers)) { zookeeperClient =>
        zookeeperClient.start()
        if (zookeeperClient.checkExists().forPath(zNode) == null) {
          logger.warning(s"Creating Zookeeper Node --> $zNode in Host --> $zkServers ")
          zookeeperClient.create().creatingParentsIfNeeded().forPath(zNode, someData.getBytes())
        } else {
          val bytesToWrite = someData.getBytes
          zookeeperClient.setData().forPath(zNode, bytesToWrite)
        }
        logger.info(s"Persisted in Node -> $zNode in Value --> $someData")
      }
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
   * Gets a connection to zookeeper through curator framework
   *
   * @param zkServers : Zookeeper hosts and port
   * @return CuratorFramework
   */
  def getZKConnection(zkServers: String): CuratorFramework = {
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    CuratorFrameworkFactory.newClient(zkServers, retryPolicy)
  }


  /**
    * Reads the Contents of a ZK Node
    *
    * @param zkServers : Zookeeper hosts and port
    * @param zNode : Fully Qualified Path of the File to Read
    * @return Content of the File
    */
  def readFromZK(zkServers: String, zNode: String): Option[String] = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper READ Request for --> \nzServers --> $zkServers \nzNode --> $zNode \n")
    var read: Option[String] = None

    try {
      GenericUtils.withResources(getZKConnection(zkServers)) { zookeeperClient =>
        zookeeperClient.start()
        if (zookeeperClient.checkExists().forPath(zNode) == null) {
          logger.warning(s"Path does not exists --> $zNode ! Will return None.")
        } else {
          read = Some(new String(zookeeperClient.getData().forPath(zNode)))
          logger.info(s"Found value --> $read")
        }
        read
      }
    } catch {
      case ex: Throwable =>
        throw ex
    }
  }

  /**
    * Delete a Node on ZK
    *
    * @param zkServers : Zookeeper hosts and port
    * @param zNode : Fully Qualified Path of the File to Write data into
    */
  def deleteNodeOnZK(zkServers: String, zNode: String): Unit = {
    def MethodName: String = new Exception().getStackTrace.apply(1).getMethodName

    logger.info(" @Begin --> " + MethodName)

    logger.info(s"Zookeeper DELETE Request for --> \nzServers --> $zkServers \nzNode --> $zNode \n")
    try {
      GenericUtils.withResources(getZKConnection(zkServers)) { zookeeperClient =>
        zookeeperClient.start()
        if (zookeeperClient.checkExists().forPath(zNode) == null) {
          logger.warning(s"Path does not exists --> $zNode ! Will delete None.")
          None
        } else {
          zookeeperClient.delete.forPath(zNode)
          logger.warning(s"Deleted Node -> $zNode")
        }
      }
    } catch {
         case ex: Throwable =>
           throw ex
    }
  }
}

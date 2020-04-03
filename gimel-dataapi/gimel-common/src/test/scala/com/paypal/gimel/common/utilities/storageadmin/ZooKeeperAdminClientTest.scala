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

package com.paypal.gimel.common.utilities.storageadmin

import org.scalatest._

import com.paypal.gimel.common.conf.GimelConstants
import com.paypal.gimel.common.storageadmin.ZooKeeperAdminClient
import com.paypal.gimel.common.utilities.zookeeper.ZooKeeperEmbedded
import com.paypal.gimel.logger.Logger

class ZooKeeperAdminClientTest extends FunSpec with Matchers with BeforeAndAfterAll {

  val DEFAULT_ZK_PORT = 2181
  var zookeeper: ZooKeeperEmbedded = _
  val zkNode = GimelConstants.DEFAULT_ZOOKEEPER_CHECKPOINT_PATH + "/test_cluster/test_user/test_app"
  val logger = Logger()
  logger.setLogLevel("INFO")
  logger.consolePrintEnabled = true

  protected override def beforeAll(): Unit = {
    logger.debug("Starting a ZooKeeper instance on port " + DEFAULT_ZK_PORT)
    zookeeper = new ZooKeeperEmbedded(DEFAULT_ZK_PORT)
    zookeeper.start()
    logger.debug("ZooKeeper instance is running at " + zookeeper.connectString())
  }

  protected override def afterAll(): Unit = {
    zookeeper.stop()
  }

  describe("writetoZK") {
    it ("should create and write data to zookeeper node specified") {
      ZooKeeperAdminClient.writetoZK(zookeeper.connectString(), zkNode, "test")
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode).get == "test")
    }

    it ("should set data to zookeeper node specified if it already exists") {
      // Making sure it exists already
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode).get == "test")
      // Updating the data in zknode
      ZooKeeperAdminClient.writetoZK(zookeeper.connectString(), zkNode, "test_updated")
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode).get == "test_updated")
    }
  }

  describe("readFromZK") {
    it ("should read data from zookeeper node specified") {
      ZooKeeperAdminClient.writetoZK(zookeeper.connectString(), zkNode + "_1", "test")
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode + "_1").get == "test")
    }

    it ("should get None if zknode specified does not exist") {
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode + "_2") == None)
    }
  }

  describe("deleteNodeOnZK") {
    it ("should delete the zookeeper node specified") {
      ZooKeeperAdminClient.writetoZK(zookeeper.connectString(), zkNode + "_2", "test")
      ZooKeeperAdminClient.deleteNodeOnZK(zookeeper.connectString(), zkNode + "_2")
      assert(ZooKeeperAdminClient.readFromZK(zookeeper.connectString(), zkNode + "_2") == None)
    }
  }
}

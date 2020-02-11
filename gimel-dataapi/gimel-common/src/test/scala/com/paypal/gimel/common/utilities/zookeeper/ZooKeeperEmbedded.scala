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

package com.paypal.gimel.common.utilities.zookeeper

import org.apache.curator.test.TestingServer

import com.paypal.gimel.logger.Logger

/**
  * This logic is borrowed from confluent kafka-streams-examples.
  * https://github.com/confluentinc/kafka-streams-examples/blob/5.4.0-post/src/test/java/io/confluent/examples/streams/zookeeper/ZooKeeperEmbedded.java
  *
  * Runs an in-memory, "embedded" instance of a ZooKeeper server.
  *
  * The ZooKeeper server instance is automatically started when you create a new instance of this class.
  */
class ZooKeeperEmbedded(port: Int) {

  private val logger = new Logger()

  var server: TestingServer = _

  def start() {
    logger.debug("Starting embedded ZooKeeper server on port " + port);
    this.server = new TestingServer(port);
  }

  def stop() {
    logger.debug("Shutting down embedded ZooKeeper server at " + server.getConnectString());
    server.close();
    logger.debug(s"Shutdown of embedded ZooKeeper server at ${server.getConnectString()} completed -> " );
  }

  /**
    * The ZooKeeper connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
    * Example: `127.0.0.1:2181`.
    *
    * You can use this to e.g. tell Kafka brokers how to connect to this instance.
    */
  def connectString(): String = {
    server.getConnectString();
  }

  /**
    * The hostname of the ZooKeeper instance.  Example: `127.0.0.1`
    */
  def hostname() : String = {
    // "server:1:2:3" -> "server:1:2"
    connectString().substring(0, connectString().lastIndexOf(':'));
  }

}

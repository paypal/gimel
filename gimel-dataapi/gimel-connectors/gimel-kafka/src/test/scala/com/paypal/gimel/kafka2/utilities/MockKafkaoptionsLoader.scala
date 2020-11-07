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

import com.paypal.gimel.common.conf.{GimelConstants, KafkaOptionsLoader}
import com.paypal.gimel.kafka2.conf.KafkaConfigs

/*
 * Mock Kafka Options Loader for unit testing
 * Returns dummy values for bootstrap.servers
 */
class MockKafkaoptionsLoader extends KafkaOptionsLoader {
  override def loadKafkaOptions(config: Map[String, String]): Map[String, Map[String, String]] = {
    assert(config.contains(GimelConstants.KAFKA_TOPICS_KEY))
    Map(config.get(GimelConstants.KAFKA_TOPICS_KEY).get ->
      Map(KafkaConfigs.kafkaServerKey -> "localhost:9092"))
  }
}
